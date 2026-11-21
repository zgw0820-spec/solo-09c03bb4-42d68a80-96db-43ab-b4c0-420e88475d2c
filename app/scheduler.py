"""回填调度核心。

模拟时钟为整数，只通过 ``advance`` 向前推进。所有状态修改集中在本模块，
FastAPI 层用一把异步锁保证并发提交不会把同一节点分给两个作业。

关键规则：

1. 作业只使用同一机架内的节点，不跨机架；队首在重新预约时可以换机架。
2. 队首需要等待时公布最早开工时刻（预约）；已公布的时刻只会提前、不会推后。
3. 队首等待期间，按提交顺序检查后续作业：能用当前空位、跑满自己的时限且
   不耽误队首预约（now + duration <= 预约时刻，且不占用预约节点）才放行。
4. 作业完工回报或达到时限时释放节点并立即重排。
5. 节点释放带归属校验：迟到的完工回报面对已结束/已转配的节点不会再次释放。

接续（MPI 后处理）规则：

6. 运行中的普通作业可挂一段接续作业（后段），读取前段留在节点本地的临时
   文件，因此后段复用前段的同一组节点；每个前段最多接一段。
7. 受理前按两段都跑满时限估算原节点释放时刻；若队首即使换机架也赶不上
   已公布的预约时刻，拒绝接续，原作业与预约不受影响。
8. 受理后、前段仍在运行期间，这些节点的预计释放时刻计入后段时限。
9. 前段的有效完工回报一经受理，后段在当前模拟时刻用这组节点开工。
10. 前段超时被杀时，后段随之取消，节点释放并重排。
"""

from __future__ import annotations

import heapq
import itertools
from dataclasses import dataclass, field
from typing import Literal

from .config import Settings

JobStatus = Literal["queued", "running", "finished", "killed", "cancelled"]


class SchedulerError(Exception):
    """调度业务错误，HTTP 层映射为 4xx。"""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


@dataclass
class Job:
    job_id: int
    nodes: int
    duration: int
    submit_time: int
    status: JobStatus = "queued"
    rack: str | None = None
    assigned_nodes: list[int] = field(default_factory=list)
    scheduled_start: int | None = None
    actual_start: int | None = None
    finish_time: int | None = None
    finish_reason: str | None = None
    # 接续链：后段的 front_id 指向前段；前段的 continuation_id 指向后段
    front_id: int | None = None
    continuation_id: int | None = None


@dataclass
class Event:
    time: int
    type: str
    detail: str
    job_id: int | None = None
    rack: str | None = None


class Scheduler:
    """单机模拟调度器（非线程安全，外层加锁）。"""

    MAX_EVENTS = 200

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.rack_names: list[str] = list(settings.rack_names)
        self.capacities: dict[str, int] = dict(
            zip(settings.rack_names, settings.rack_capacities, strict=True)
        )
        self.now: int = 0
        self.jobs: dict[int, Job] = {}
        self.queue: list[int] = []  # 提交顺序的待调度作业 id
        # rack -> 节点号 -> 当前占用作业 id
        self.owners: dict[str, dict[int, int]] = {
            rack: {} for rack in self.rack_names
        }
        # (释放时刻, 序号, 作业 id)
        self._timeout_heap: list[tuple[int, int, int]] = []
        self._seq = itertools.count()
        self._next_id = 1
        self._events: list[Event] = []

    # ------------------------------------------------------------------ 工具

    def _log(self, type_: str, detail: str, job_id: int | None = None,
             rack: str | None = None) -> Event:
        event = Event(time=self.now, type=type_, detail=detail,
                      job_id=job_id, rack=rack)
        self._events.append(event)
        if len(self._events) > self.MAX_EVENTS:
            del self._events[: -self.MAX_EVENTS]
        return event

    def _job(self, job_id: int) -> Job:
        job = self.jobs.get(job_id)
        if job is None:
            raise SchedulerError(404, f"作业 {job_id} 不存在")
        return job

    # 假设性节点释放时刻：(机架, 节点号) -> 释放时刻，用于受理接续前的评估
    FreeOverrides = dict[tuple[str, int], int]

    def _node_free_at(self, rack: str, node: int,
                      overrides: FreeOverrides | None = None) -> int:
        """节点在模拟时间轴上变为空闲的时刻（空闲节点取当前时刻）。

        占用作业若带有已受理、尚未开工的接续后段，则释放时刻把后段时限
        也计入（两段都跑满时限的最坏估计）。
        """
        if overrides is not None and (rack, node) in overrides:
            return overrides[(rack, node)]
        owner_id = self.owners[rack].get(node)
        if owner_id is None:
            return self.now
        owner = self.jobs[owner_id]
        assert owner.actual_start is not None
        free_at = owner.actual_start + owner.duration
        if owner.continuation_id is not None:
            cont = self.jobs[owner.continuation_id]
            if cont.status == "queued":  # 后段待开工：释放时刻计入后段时限
                free_at += cont.duration
        return free_at

    def _earliest_on_rack(self, rack: str, need: int,
                          overrides: FreeOverrides | None = None,
                          ) -> tuple[int, list[int]]:
        """某机架上凑齐 need 个节点的最早时刻及对应节点集合。"""
        cap = self.capacities[rack]
        free_at = sorted(
            (self._node_free_at(rack, n, overrides), n) for n in range(cap)
        )
        chosen = free_at[:need]
        earliest = max(self.now, max(t for t, _ in chosen))
        return earliest, [n for _, n in chosen]

    def _free_nodes_now(self, rack: str) -> list[int]:
        return [n for n in range(self.capacities[rack])
                if n not in self.owners[rack]]

    # --------------------------------------------------------------- 作业启动

    def _start(self, job: Job, rack: str, nodes: list[int],
               kind: Literal["direct", "backfill", "reservation",
                             "early"]) -> Event:
        assert job.status == "queued"
        owner_map = self.owners[rack]
        for n in nodes:
            if n in owner_map:
                # 理论不可达：上层已校验，作为并发/逻辑的最后防线
                raise SchedulerError(409, f"节点 {rack}[{n}] 已被占用")
        for n in nodes:
            owner_map[n] = job.job_id
        job.status = "running"
        job.rack = rack
        job.assigned_nodes = sorted(nodes)
        job.actual_start = self.now
        self.queue.remove(job.job_id)
        heapq.heappush(
            self._timeout_heap,
            (self.now + job.duration, next(self._seq), job.job_id),
        )
        labels = {
            "direct": "直接开工",
            "backfill": "回填开工",
            "reservation": "按预约开工",
            "early": "提前于预约开工",
        }
        return self._log("job_started",
                         f"作业 {job.job_id} 在机架 {rack} 节点 {sorted(nodes)} "
                         f"{labels[kind]}，时限 {job.duration}，"
                         f"预计释放时刻 {self.now + job.duration}",
                         job.job_id, rack)

    # --------------------------------------------------------------- 主重排逻辑

    def reschedule(self) -> list[Event]:
        """核心回填重排，返回本次新产生的事件。"""
        events: list[Event] = []
        # 队首能立即开工就持续开工，直到出现需要等待的队首
        while self.queue:
            head = self.jobs[self.queue[0]]
            best = self._earliest_start(head)
            earliest, rack, nodes = best
            if earliest > self.now:
                # 队首需要等待：公布/刷新预约（时刻只可提前）
                self._publish_reservation(head, earliest, rack, nodes, events)
                # 回填：按提交顺序检查后续作业
                events.extend(self._backfill_pass(head))
                break
            kind = "direct" if head.scheduled_start is None else (
                "early" if head.scheduled_start > self.now else "reservation"
            )
            events.append(self._start(head, rack, nodes, kind))
        return events

    def _earliest_start(self, job: Job,
                        overrides: FreeOverrides | None = None,
                        ) -> tuple[int, str, list[int]]:
        """跨所有机架求最早开工时刻（队首可换机架）。

        ``overrides`` 用于假设性评估（如受理接续前把前段节点释放时刻
        延后到两段跑满），不修改任何实际状态。
        """
        candidates: list[tuple[int, int, str, list[int]]] = []
        for idx, rack in enumerate(self.rack_names):
            if self.capacities[rack] < job.nodes:
                continue
            earliest, nodes = self._earliest_on_rack(rack, job.nodes, overrides)
            candidates.append((earliest, idx, rack, nodes))
        if not candidates:
            biggest = max(self.capacities.values())
            raise SchedulerError(
                422,
                f"作业需要 {job.nodes} 个同机架节点，"
                f"最大机架仅 {biggest} 个节点，无法调度",
            )
        earliest, _, rack, nodes = min(candidates, key=lambda c: (c[0], c[1]))
        return earliest, rack, nodes

    def _publish_reservation(self, head: Job, earliest: int, rack: str,
                             nodes: list[int], events: list[Event]) -> None:
        old = head.scheduled_start
        # 防御性保证：已公布的开工时刻只可提前
        new_time = earliest if old is None else min(old, earliest)
        head.scheduled_start = new_time
        head.rack = rack
        head.assigned_nodes = sorted(nodes)
        if old is None:
            events.append(self._log(
                "reservation_published",
                f"队首作业 {head.job_id} 需等待，公布最早开工时刻 {new_time}，"
                f"预约机架 {rack} 节点 {sorted(nodes)}，"
                f"预计释放时刻 {new_time + head.duration}",
                head.job_id, rack,
            ))
        elif new_time < old:
            events.append(self._log(
                "reservation_updated",
                f"作业 {head.job_id} 预约开工时刻提前：{old} -> {new_time}",
                head.job_id, rack,
            ))

    def _backfill_pass(self, head: Job) -> list[Event]:
        """队首等待时，按提交顺序为后续作业找当前空位回填。"""
        events: list[Event] = []
        assert head.scheduled_start is not None
        reserved_rack = head.rack
        reserved_nodes = set(head.assigned_nodes)
        for job_id in list(self.queue[1:]):
            job = self.jobs[job_id]
            if job.status != "queued":
                continue
            # 跑满时限也不耽误预约
            if self.now + job.duration > head.scheduled_start:
                continue
            for rack in self.rack_names:
                if self.capacities[rack] < job.nodes:
                    continue
                free_now = self._free_nodes_now(rack)
                if rack == reserved_rack:
                    free_now = [n for n in free_now
                                if n not in reserved_nodes]
                if len(free_now) >= job.nodes:
                    picked = sorted(free_now[: job.nodes])
                    events.append(self._start(job, rack, picked, "backfill"))
                    break
        return events

    # ------------------------------------------------------------------ 提交

    def submit(self, nodes: int, duration: int) -> tuple[Job, list[Event]]:
        max_cap = max(self.capacities.values())
        if nodes > max_cap:
            raise SchedulerError(
                422,
                f"作业需要 {nodes} 个同机架节点，最大机架仅 {max_cap} 个",
            )
        job = Job(
            job_id=self._next_id,
            nodes=nodes,
            duration=duration,
            submit_time=self.now,
        )
        self._next_id += 1
        self.jobs[job.job_id] = job
        self.queue.append(job.job_id)
        events = [self._log(
            "job_submitted",
            f"提交作业 {job.job_id}：需 {nodes} 节点、时限 {duration}",
            job.job_id,
        )]
        events.extend(self.reschedule())
        return job, events

    # ------------------------------------------------------------- 接续（后段）

    def submit_continuation(self, front_id: int, duration: int,
                            ) -> tuple[Job, Job, list[Event]]:
        """为运行中的普通作业挂接续后段（MPI 后处理复用同组节点）。

        受理前按两段都跑满时限估算原节点释放时刻；若队首即使换机架也
        赶不上已公布的预约时刻，拒绝接续，原作业与预约保持不变。
        返回 (前段, 后段, 事件)。
        """
        front = self._job(front_id)
        if front.front_id is not None:
            raise SchedulerError(
                409,
                f"作业 {front_id} 本身是接续后段（前段 {front.front_id}），"
                "不能再挂接续",
            )
        if front.status != "running":
            raise SchedulerError(
                409,
                f"前段作业 {front_id} 当前状态为 {front.status}，"
                "仅运行中的普通作业可挂接续",
            )
        if front.continuation_id is not None:
            raise SchedulerError(
                409,
                f"前段作业 {front_id} 已有接续后段 {front.continuation_id}，"
                "每个前段最多接一段",
            )
        assert front.actual_start is not None and front.rack is not None

        # 预约冲突评估：两段都跑满时限时原节点的释放时刻
        extended = front.actual_start + front.duration + duration
        if self.queue:
            head = self.jobs[self.queue[0]]
            if head.scheduled_start is not None:
                overrides: Scheduler.FreeOverrides = {
                    (front.rack, n): extended for n in front.assigned_nodes
                }
                earliest, _, _ = self._earliest_start(head, overrides)
                if earliest > head.scheduled_start:
                    raise SchedulerError(
                        409,
                        f"接续后节点预计释放时刻将延至 {extended}，队首作业 "
                        f"{head.job_id} 即使换机架最早也要 {earliest} 开工，"
                        f"赶不上已公布的预约 {head.scheduled_start}，拒绝接续；"
                        "原作业与预约不变",
                    )

        cont = Job(
            job_id=self._next_id,
            nodes=front.nodes,
            duration=duration,
            submit_time=self.now,
            front_id=front.job_id,
        )
        # 后段复用前段同组节点（读取节点本地临时文件），受理即锁定机架与节点
        cont.rack = front.rack
        cont.assigned_nodes = sorted(front.assigned_nodes)
        self._next_id += 1
        self.jobs[cont.job_id] = cont
        front.continuation_id = cont.job_id
        events = [self._log(
            "continuation_accepted",
            f"受理接续后段 {cont.job_id}：前段 {front.job_id} 完工后在机架 "
            f"{front.rack} 节点 {cont.assigned_nodes} 接续运行，时限 {duration}；"
            f"这些节点预计释放时刻计入后段时限，延至 {extended}",
            cont.job_id, front.rack,
        )]
        # 受理后这些节点的预计释放时刻已变化，重排以刷新预约视图
        events.extend(self.reschedule())
        return front, cont, events

    def _handoff(self, front: Job, at: int) -> list[Event]:
        """前段完工交接：后段在当前模拟时刻用同一组节点开工。"""
        cont = self.jobs[front.continuation_id]  # type: ignore[index]
        assert cont.status == "queued"
        rack = front.rack
        assert rack is not None
        owner_map = self.owners[rack]
        front.status = "finished"
        front.finish_time = at
        front.finish_reason = "completed"
        events = [self._log(
            "job_completed",
            f"作业 {front.job_id} 于时刻 {at} 完工，机架 {rack} 节点 "
            f"{front.assigned_nodes} 直接交接给接续后段 {cont.job_id}",
            front.job_id, rack,
        )]
        # 节点归属过户：前段 -> 后段（带归属校验，只过户仍属前段的节点）
        for n in front.assigned_nodes:
            if owner_map.get(n) == front.job_id:
                owner_map[n] = cont.job_id
        cont.status = "running"
        cont.actual_start = self.now
        heapq.heappush(
            self._timeout_heap,
            (self.now + cont.duration, next(self._seq), cont.job_id),
        )
        events.append(self._log(
            "job_started",
            f"接续后段 {cont.job_id} 于当前时刻 {self.now} 在机架 {rack} 节点 "
            f"{cont.assigned_nodes} 开工（承接前段 {front.job_id}），"
            f"时限 {cont.duration}，预计释放时刻 {self.now + cont.duration}",
            cont.job_id, rack,
        ))
        return events

    # ------------------------------------------------------------- 完工/超时释放

    def _release(self, job: Job, reason: Literal["completed", "killed"],
                 at: int) -> list[Event]:
        assert job.status == "running"
        rack = job.rack
        assert rack is not None
        owner_map = self.owners[rack]
        # 归属校验：只释放仍属于本作业的节点，防止陈旧回报误释放转配节点
        for n in job.assigned_nodes:
            if owner_map.get(n) == job.job_id:
                del owner_map[n]
        job.status = "finished" if reason == "completed" else "killed"
        job.finish_time = at
        job.finish_reason = reason
        etype = "job_completed" if reason == "completed" else "job_timeout"
        label = "完工释放" if reason == "completed" else "达到时限被杀并释放"
        events = [self._log(
            etype,
            f"作业 {job.job_id} 于时刻 {at} {label}机架 {rack} 节点 "
            f"{job.assigned_nodes}",
            job.job_id, rack,
        )]
        # 前段超时：已受理但未开工的接续后段随之取消
        if reason == "killed" and job.continuation_id is not None:
            cont = self.jobs[job.continuation_id]
            if cont.status == "queued":
                cont.status = "cancelled"
                cont.finish_time = at
                cont.finish_reason = "cancelled"
                events.append(self._log(
                    "continuation_cancelled",
                    f"前段 {job.job_id} 超时被杀，接续后段 {cont.job_id} "
                    "取消（节点本地临时文件随前段一并清理）",
                    cont.job_id, rack,
                ))
        return events

    def report_completion(self, job_id: int, at: int) -> tuple[Job, list[Event]]:
        """处理完工回报。

        回报时刻 ``at`` 不得早于作业实际开工时刻；可以在时钟推进到某时刻后
        补报该时刻的完工（``at`` 不得晚于当前模拟时刻），也不得晚于运行时限
        （超时的作业已被杀掉，回报按迟到处理）。
        """
        job = self._job(job_id)
        if job.status != "running":
            raise SchedulerError(
                409,
                f"作业 {job_id} 当前状态为 {job.status}，完工回报无效；"
                "迟到的回报不会释放已转配的节点",
            )
        if at > self.now:
            raise SchedulerError(
                409,
                f"当前模拟时刻为 {self.now}，完工回报时刻 {at} 不能晚于当前时刻",
            )
        assert job.actual_start is not None
        if at < job.actual_start:
            raise SchedulerError(409, "完工时刻早于实际开工时刻")
        if at > job.actual_start + job.duration:
            raise SchedulerError(
                409,
                f"完工时刻 {at} 已超过运行时限 "
                f"{job.actual_start + job.duration}，作业应被时限杀掉",
            )
        # 前段带已受理的接续后段：完工回报受理后，后段在当前时刻接管节点开工
        if (job.continuation_id is not None
                and self.jobs[job.continuation_id].status == "queued"):
            events = self._handoff(job, at)
        else:
            events = self._release(job, "completed", at)
        events.extend(self.reschedule())
        return job, events

    def _process_timeouts(self, until: int) -> list[Event]:
        """处理释放时刻 <= until 的所有超时。

        按释放时刻逐批弹出，并在每个时刻立即重排；重排中新启动的回填作业
        若时限也落在 ``until`` 之前，会在后续循环中被继续处理。堆中条目
        可能已陈旧（作业提前完工），通过作业状态与实际释放时刻过滤。
        """
        events: list[Event] = []
        while self._timeout_heap and self._timeout_heap[0][0] <= until:
            t = self._timeout_heap[0][0]
            due_ids: list[int] = []
            while self._timeout_heap and self._timeout_heap[0][0] == t:
                _, _, jid = heapq.heappop(self._timeout_heap)
                due_ids.append(jid)
            self.now = t
            for jid in due_ids:
                job = self.jobs[jid]
                # 陈旧条目：已完工或实际释放时刻已变化，跳过
                if job.status != "running":
                    continue
                if job.actual_start + job.duration != t:
                    continue
                events.extend(self._release(job, "killed", t))
            # 每个释放时刻都立即重排，回填作业可能在该时刻入场
            events.extend(self.reschedule())
        return events

    # ------------------------------------------------------------------ 时钟

    def advance(self, steps: int) -> tuple[int, int, list[Event]]:
        if steps <= 0:
            raise SchedulerError(422, "steps 必须为正整数")
        start = self.now
        target = start + steps
        events = self._process_timeouts(target)
        if self.now < target:
            self.now = target
            events.extend(self.reschedule())
        return start, target, events

    def reset(self) -> None:
        self.now = 0
        self.jobs.clear()
        self.queue.clear()
        for m in self.owners.values():
            m.clear()
        self._timeout_heap.clear()
        self._seq = itertools.count()
        self._next_id = 1
        self._events.clear()
        self._log("sim_reset", "模拟环境已重置")

    # ------------------------------------------------------------------ 查询视图

    def reservations(self) -> list[dict]:
        out = []
        for jid in self.queue:
            job = self.jobs[jid]
            if job.scheduled_start is not None:
                out.append({
                    "job_id": job.job_id,
                    "rack": job.rack,
                    "nodes": list(job.assigned_nodes),
                    "scheduled_start": job.scheduled_start,
                    "deadline": job.scheduled_start + job.duration,
                })
        return out

    def chained_continuations(self) -> list[Job]:
        """已受理、等待前段完工的接续后段（按 id 排序）。"""
        return sorted(
            (j for j in self.jobs.values()
             if j.front_id is not None and j.status == "queued"),
            key=lambda j: j.job_id,
        )

    def rack_occupancy(self) -> list[dict]:
        result = []
        for rack in self.rack_names:
            owner_map = self.owners[rack]
            nodes = {n: owner_map.get(n)
                     for n in range(self.capacities[rack])}
            used = len(owner_map)
            result.append({
                "rack": rack,
                "capacity": self.capacities[rack],
                "used": used,
                "free": self.capacities[rack] - used,
                "nodes": nodes,
            })
        return result

    def recent_events(self, limit: int = 20) -> list[Event]:
        return list(self._events[-limit:])
