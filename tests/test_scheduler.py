"""调度器单元测试：回填、预约、跨时刻推进、陈旧回报。"""

from __future__ import annotations

import pytest

from app.config import Settings
from app.scheduler import Scheduler, SchedulerError


def make_scheduler(racks: tuple[int, ...] = (4, 4)) -> Scheduler:
    return Scheduler(Settings(rack_capacities=racks))


def submit(s: Scheduler, nodes: int, duration: int) -> int:
    job, _ = s.submit(nodes, duration)
    return job.job_id


# --------------------------------------------------------------- 基础提交与占用


def test_submit_jobs_fill_racks_and_head_publishes_reservation():
    s = make_scheduler((4, 4))
    j1 = submit(s, 4, 10)
    j2 = submit(s, 4, 10)
    j3 = submit(s, 4, 5)

    j = s.jobs
    assert j[j1].status == "running" and j[j1].rack == "r1"
    assert j[j2].status == "running" and j[j2].rack == "r2"
    # 两个机架都满，队首等待并公布最早开工时刻
    assert j[j3].status == "queued"
    assert j[j3].scheduled_start == 10
    assert j[j3].rack in ("r1", "r2")
    assert len(j[j3].assigned_nodes) == 4
    occ = {r["rack"]: r for r in s.rack_occupancy()}
    assert occ["r1"]["used"] == occ["r2"]["used"] == 4
    assert {r["job_id"] for r in s.reservations()} == {j3}


def test_oversized_job_rejected():
    s = make_scheduler((3, 2))
    with pytest.raises(SchedulerError) as ei:
        s.submit(4, 1)
    assert ei.value.status_code == 422


# ----------------------------------------------------------------- 回填逻辑


def test_backfill_uses_free_space_without_delaying_reservation():
    s = make_scheduler((4, 4))
    submit(s, 4, 10)                  # 占满 r1
    submit(s, 3, 10)                  # r2 占用 0,1,2，节点 3 空闲
    waiting = submit(s, 4, 10)        # 队首：r1/r2 均在 t=10 才可凑齐
    assert s.jobs[waiting].scheduled_start == 10
    # 短作业：现在空位（r2 节点 3）上跑满时限也不晚于预约 → 回填
    short_ok = submit(s, 1, 3)
    # 跑满时限会超过预约时刻 → 不允许回填
    too_long = submit(s, 1, 12)

    j = s.jobs
    assert j[short_ok].status == "running"
    assert j[short_ok].rack == "r2"
    assert j[short_ok].assigned_nodes == [3]
    assert j[too_long].status == "queued"
    assert j[too_long].scheduled_start is None
    # 预约仍为 10，未被推迟
    assert j[waiting].scheduled_start == 10


def test_backfill_boundary_duration_equals_reservation_time_allowed():
    s = make_scheduler((4, 4))
    submit(s, 4, 10)                  # r1 满
    submit(s, 3, 10)                  # r2 节点 0,1,2
    waiting = submit(s, 4, 10)        # 预约 t=10
    # now=0，duration=10 => 完工时刻 10 <= 预约 10，边界放行
    exact = submit(s, 1, 10)
    assert s.jobs[exact].status == "running"
    assert s.jobs[exact].rack == "r2" and s.jobs[exact].assigned_nodes == [3]
    assert s.jobs[waiting].scheduled_start == 10


def test_backfill_cannot_take_reserved_nodes():
    s = make_scheduler((4,))
    submit(s, 3, 10)                  # r1 节点 0,1,2 被占
    head = submit(s, 4, 10)           # 需 4 节点，预约 t=10，含节点 3
    head_job = s.jobs[head]
    assert head_job.scheduled_start == 10
    assert set(head_job.assigned_nodes) == {0, 1, 2, 3}
    # 节点 3 当前空闲但已被预约，短作业不得占用
    thief = submit(s, 1, 5)
    assert s.jobs[thief].status == "queued"
    assert s.rack_occupancy()[0]["used"] == 3


# ---------------------------------------------------- 已公布时刻只可提前 + 换机架


def test_head_can_switch_rack_and_start_earlier_than_published():
    s = make_scheduler((3, 3))
    j1 = submit(s, 3, 10)             # r1 全占，t=10 释放
    submit(s, 2, 4)                   # r2 节点 0,1，t=4 释放
    head = submit(s, 3, 10)           # 队首：r2 t=4 可凑齐
    h = s.jobs[head]
    assert h.scheduled_start == 4 and h.rack == "r2"
    # r1 作业在 t=2 提前完工：r1 此刻即可凑齐 3 节点 → 立即开工、换到 r1
    s.advance(2)
    s.report_completion(j1, 2)
    assert h.status == "running"
    assert h.actual_start == 2
    assert h.rack == "r1"
    # 实际开工（2）早于此前公布的时刻（4），公布值本身从不推后
    assert h.actual_start <= h.scheduled_start == 4


def test_head_starts_immediately_when_space_frees():
    s = make_scheduler((4, 4))
    j1 = submit(s, 4, 10)
    submit(s, 4, 10)
    j3 = submit(s, 4, 10)
    assert s.jobs[j3].scheduled_start == 10
    # t=5 完工释放 r1，队首立即开工（早于预约 10）
    s.advance(5)
    s.report_completion(j1, 5)
    h = s.jobs[j3]
    assert h.status == "running"
    assert h.actual_start == 5 and h.rack == "r1"
    assert h.scheduled_start == 10      # 历史预约保留


# ------------------------------------------------------- 跨时刻推进与时限释放


def test_advance_crossing_multiple_release_points_reschedules_each():
    s = make_scheduler((4, 4))
    a = submit(s, 4, 2)                # r1，t=2 时限
    b = submit(s, 4, 4)                # r2，t=4 时限
    c = submit(s, 4, 10)               # 预约 r1 t=2
    d = submit(s, 1, 2)                # 暂无空位，排队
    assert s.jobs[c].scheduled_start == 2
    assert s.jobs[d].status == "queued"

    # 一次推进直接跨过 t=2、t=4、t=6 三个释放时刻
    _, target, events = s.advance(6)
    assert target == 6
    types = [e.type for e in events]
    # t=2 a 超时、c 开工；t=4 b 超时、d 开工；t=6 d 超时
    assert types.count("job_timeout") == 3
    assert types.count("job_started") == 2
    jc, jd = s.jobs[c], s.jobs[d]
    assert jc.status == "running" and jc.actual_start == 2   # 时限 t=12 未到
    assert jd.actual_start == 4
    assert jd.status == "killed" and jd.finish_time == 6
    assert s.rack_occupancy()[1]["used"] == 0


def test_timeout_kills_and_releases_then_reschedules():
    s = make_scheduler((4,))
    j = submit(s, 2, 3)
    s.advance(3)
    assert s.jobs[j].status == "killed"
    assert s.jobs[j].finish_reason == "killed"
    assert s.jobs[j].finish_time == 3
    assert s.rack_occupancy()[0]["used"] == 0


def test_backfill_started_during_advance_also_times_out_within_window():
    # 重排中新启动的作业，其时限也落在推进窗口内时应被继续处理
    s = make_scheduler((2, 2))
    submit(s, 2, 2)                    # r1，t=2 释放
    submit(s, 2, 8)                    # r2，t=8 释放
    submit(s, 2, 5)                    # 预约 r1 t=2，t=7 到期
    short = submit(s, 1, 2)            # 无空位，排队
    s.advance(6)
    # t=2 队首占 r1，r2 仍满 → short 继续等待
    assert s.jobs[short].status == "queued"
    s.advance(3)                       # 目标 t=9：t=7、t=8 各有释放
    assert s.jobs[short].actual_start == 7
    assert s.jobs[short].status == "killed"
    assert s.jobs[short].finish_time == 9


# ----------------------------------------------------------- 迟到完工回报处理


def test_late_completion_after_timeout_does_not_touch_reassigned_nodes():
    s = make_scheduler((4,))
    old = submit(s, 2, 3)
    s.advance(3)                        # 时限杀掉，节点释放
    assert s.jobs[old].status == "killed"
    new = submit(s, 2, 5)              # 节点转配给新作业
    assert s.jobs[new].status == "running"
    with pytest.raises(SchedulerError) as ei:
        s.report_completion(old, 3)    # 迟到回报被拒
    assert ei.value.status_code == 409
    # 新作业的节点占用原样保留
    assert s.jobs[new].status == "running"
    assert s.rack_occupancy()[0]["used"] == 2
    assert all(v == new for v in s.owners["r1"].values())


def test_duplicate_completion_is_stale():
    s = make_scheduler((4,))
    j = submit(s, 2, 5)
    s.advance(2)
    s.report_completion(j, 2)
    assert s.jobs[j].status == "finished"
    with pytest.raises(SchedulerError):
        s.report_completion(j, 2)


def test_completion_at_cannot_be_in_future_or_before_start():
    s = make_scheduler((4,))
    j = submit(s, 2, 5)
    with pytest.raises(SchedulerError):
        s.report_completion(j, 3)       # 当前时刻为 0，回报未来时刻
    s.advance(4)
    job, _ = s.report_completion(j, 4)  # 与时钟一致且未超时限，放行
    assert job.status == "finished"


def test_completion_beyond_deadline_rejected():
    s = make_scheduler((4,))
    j = submit(s, 2, 3)
    s.advance(3)                        # 已超时
    with pytest.raises(SchedulerError):
        s.report_completion(j, 3)


# ---------------------------------------------------------------- 杂项


def test_reset_clears_everything():
    s = make_scheduler((4,))
    submit(s, 4, 10)
    submit(s, 1, 2)
    s.advance(2)
    s.reset()
    assert s.now == 0
    assert s.jobs == {} and s.queue == []
    assert all(m == {} for m in s.owners.values())


def test_event_log_is_bounded():
    s = make_scheduler((4,))
    for _ in range(Scheduler.MAX_EVENTS + 50):
        s.reset()
        submit(s, 1, 1)
    assert len(s._events) <= Scheduler.MAX_EVENTS


# ----------------------------------------------------------------- 接续（后段）


def continue_(s: Scheduler, front_id: int, duration: int) -> int:
    _, cont, _ = s.submit_continuation(front_id, duration)
    return cont.job_id


def test_continuation_acceptance_counts_toward_node_release_time():
    s = make_scheduler((4,))
    front = submit(s, 4, 10)               # r1 全部节点，时限 t=10
    cont = continue_(s, front, 5)

    j = s.jobs
    assert j[cont].status == "queued"      # 后段等待前段完工
    assert j[cont].front_id == front
    assert j[cont].rack == "r1" and j[cont].assigned_nodes == [0, 1, 2, 3]
    assert j[front].continuation_id == cont
    # 受理后节点预计释放时刻计入后段时限：10 + 5 = 15
    assert s._node_free_at("r1", 0) == 15
    # 新提交的队首预约按 15 估算，而非前段自身的 10
    head = submit(s, 4, 4)
    assert j[head].scheduled_start == 15
    assert {c.job_id for c in s.chained_continuations()} == {cont}


def test_handoff_on_front_completion_starts_continuation_now():
    s = make_scheduler((4,))
    front = submit(s, 4, 10)
    cont = continue_(s, front, 5)
    head = submit(s, 4, 4)                 # 预约 t=15
    assert s.jobs[head].scheduled_start == 15

    s.advance(3)                           # 当前时刻 t=3
    s.report_completion(front, 3)          # 有效完工回报 -> 交接
    j = s.jobs
    assert j[front].status == "finished"
    assert j[front].finish_time == 3
    # 后段在当前模拟时刻（t=3）用同一组节点开工
    assert j[cont].status == "running"
    assert j[cont].actual_start == 3
    assert j[cont].assigned_nodes == [0, 1, 2, 3]
    # 节点占用直接过户给后段
    assert all(v == cont for v in s.owners["r1"].values())
    assert s.rack_occupancy()[0]["used"] == 4
    # 队首预约随交接提前：后段 t=8 释放
    assert j[head].scheduled_start == 8

    # 后段自身完工回报走正常路径，释放节点、队首提前开工
    s.advance(2)                           # t=5
    s.report_completion(cont, 5)
    assert j[cont].status == "finished" and j[cont].finish_time == 5
    assert j[head].status == "running" and j[head].actual_start == 5
    assert all(v == head for v in s.owners["r1"].values())


def test_front_timeout_cancels_continuation_and_reschedules():
    s = make_scheduler((4,))
    front = submit(s, 4, 3)
    cont = continue_(s, front, 5)
    head = submit(s, 2, 4)                 # 预约 t=3+5=8
    assert s.jobs[head].scheduled_start == 8

    s.advance(3)                           # 前段在 t=3 达到时限被杀
    j = s.jobs
    assert j[front].status == "killed" and j[front].finish_time == 3
    # 后段被取消，节点释放，队首在释放时刻立即开工
    assert j[cont].status == "cancelled"
    assert j[cont].finish_time == 3 and j[cont].finish_reason == "cancelled"
    assert j[head].status == "running" and j[head].actual_start == 3
    assert all(v == head for v in s.owners["r1"].values())
    types = [e.type for e in s.recent_events(10)]
    assert "job_timeout" in types and "continuation_cancelled" in types


def test_continuation_rejected_when_head_misses_published_reservation():
    s = make_scheduler((4,))
    front = submit(s, 4, 10)               # r1，t=10 释放
    head = submit(s, 4, 10)                # 队首预约 t=10（只有 r1 可用）
    assert s.jobs[head].scheduled_start == 10

    with pytest.raises(SchedulerError) as ei:
        s.submit_continuation(front, 1)    # 释放延至 11 > 预约 10
    assert ei.value.status_code == 409
    # 原作业与预约照旧
    j = s.jobs
    assert j[front].status == "running" and j[front].continuation_id is None
    assert j[head].scheduled_start == 10
    assert len(j) == 2                     # 没有创建任何后段
    assert s.rack_occupancy()[0]["used"] == 4


def test_continuation_accepted_when_head_can_switch_rack():
    s = make_scheduler((4, 4))
    front = submit(s, 4, 10)               # r1
    submit(s, 4, 10)                       # r2
    head = submit(s, 4, 10)                # 预约 t=10，初始在 r1
    assert s.jobs[head].rack == "r1"

    # 接续使 r1 释放延至 15，但队首换到 r2 仍赶上已公布的 t=10 -> 受理
    cont = continue_(s, front, 5)
    j = s.jobs
    assert j[cont].status == "queued"
    assert j[head].scheduled_start == 10   # 已公布时刻不推后
    assert j[head].rack == "r2"            # 预约换到 r2
    assert s._node_free_at("r1", 0) == 15


def test_continuation_boundary_extension_equal_to_reservation_allowed():
    s = make_scheduler((4, 4))
    front = submit(s, 4, 10)               # r1，t=10 释放
    submit(s, 2, 5)                        # r2 占 2 节点，t=5 释放
    head = submit(s, 4, 10)                # 预约：r2 t=5 凑齐 4 节点
    assert s.jobs[head].scheduled_start == 5
    # 前段延长 0 个单位……duration>=1，延至 11；r2 仍 t=5 -> 边界放行
    cont = continue_(s, front, 1)
    assert s.jobs[cont].status == "queued"
    assert s.jobs[head].scheduled_start == 5


def test_continuation_rejected_for_non_running_or_missing_front():
    s = make_scheduler((4,))
    front = submit(s, 4, 10)
    queued = submit(s, 4, 10)              # 队首等待
    with pytest.raises(SchedulerError) as ei:
        s.submit_continuation(queued, 3)   # 未运行
    assert ei.value.status_code == 409
    with pytest.raises(SchedulerError) as ei:
        s.submit_continuation(9999, 3)     # 不存在
    assert ei.value.status_code == 404
    s.report_completion(front, 0)          # 前段完工（无后段）
    with pytest.raises(SchedulerError) as ei:
        s.submit_continuation(front, 3)    # 已完工
    assert ei.value.status_code == 409


def test_one_continuation_per_front_and_no_chaining():
    s = make_scheduler((4,))
    front = submit(s, 4, 10)
    cont = continue_(s, front, 5)
    with pytest.raises(SchedulerError) as ei:
        s.submit_continuation(front, 3)    # 每个前段最多接一段
    assert ei.value.status_code == 409
    s.report_completion(front, 0)          # 交接，后段开工
    assert s.jobs[cont].status == "running"
    with pytest.raises(SchedulerError) as ei:
        s.submit_continuation(cont, 3)     # 后段本身不能再挂接续
    assert ei.value.status_code == 409


def test_continuation_itself_times_out_normally():
    s = make_scheduler((4,))
    front = submit(s, 4, 10)
    cont = continue_(s, front, 3)
    s.report_completion(front, 0)          # t=0 交接，后段时限 t=3
    assert s.jobs[cont].status == "running"
    s.advance(3)
    j = s.jobs
    assert j[cont].status == "killed"
    assert j[cont].finish_time == 3 and j[cont].finish_reason == "killed"
    assert s.rack_occupancy()[0]["used"] == 0


def test_late_completion_after_front_timeout_keeps_cancellation():
    s = make_scheduler((4,))
    front = submit(s, 4, 3)
    cont = continue_(s, front, 5)
    s.advance(3)                           # 前段超时，后段已取消
    with pytest.raises(SchedulerError) as ei:
        s.report_completion(front, 3)      # 迟到回报被拒
    assert ei.value.status_code == 409
    assert s.jobs[cont].status == "cancelled"
    assert s.rack_occupancy()[0]["used"] == 0


def test_backfilled_completion_report_hands_off_at_current_time():
    # 补报历史完工时刻：后段仍在"当前模拟时刻"开工，而非回报的历史时刻
    s = make_scheduler((4,))
    front = submit(s, 4, 10)
    cont = continue_(s, front, 5)
    s.advance(4)                           # 当前时刻 t=4
    s.report_completion(front, 2)          # 补报 t=2 的完工
    j = s.jobs
    assert j[front].status == "finished" and j[front].finish_time == 2
    assert j[cont].status == "running"
    assert j[cont].actual_start == 4       # 当前模拟时刻开工
    assert all(v == cont for v in s.owners["r1"].values())
    # 后段时限从交接时刻起算：t=9 超时
    s.advance(5)
    assert j[cont].status == "killed" and j[cont].finish_time == 9
