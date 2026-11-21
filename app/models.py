"""Pydantic 请求/响应模型。"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

JobStatus = Literal["queued", "running", "finished", "killed", "cancelled"]


class JobSubmitRequest(BaseModel):
    """提交作业。

    - ``nodes``: 同一机架内所需的节点数（作业不跨机架）。
    - ``duration``: 运行时限（模拟时钟单位，必须为正整数）。
    """

    nodes: int = Field(gt=0, description="同一机架所需节点数")
    duration: int = Field(gt=0, description="运行时限（模拟时钟单位）")


class JobView(BaseModel):
    job_id: int
    nodes: int
    duration: int
    submit_time: int
    status: JobStatus
    rack: str | None = Field(default=None, description="实际分配的机架")
    assigned_nodes: list[int] = Field(default_factory=list)
    scheduled_start: int | None = Field(
        default=None, description="公布的最早开工时刻（预约）"
    )
    actual_start: int | None = Field(default=None, description="实际开工时刻")
    finish_time: int | None = Field(
        default=None, description="完工或达到时限的时刻"
    )
    finish_reason: str | None = Field(
        default=None,
        description="completed（完工回报）/ killed（达到时限被杀）/ "
                    "cancelled（前段超时，接续后段被取消）",
    )
    front_id: int | None = Field(
        default=None, description="接续后段指向其前段作业 id"
    )
    continuation_id: int | None = Field(
        default=None, description="前段作业指向其已受理的接续后段 id"
    )


class ContinuationRequest(BaseModel):
    """为运行中的普通作业提交接续后段（MPI 后处理复用同组节点）。

    - ``duration``: 后段运行时限（模拟时钟单位，必须为正整数）。
    - 受理前按两段都跑满时限估算原节点释放时刻；若队首即使换机架也
      赶不上已公布的预约时刻，拒绝接续（409），原作业与预约不变。
    - 每个前段最多接一段；后段本身不能再挂接续。
    """

    duration: int = Field(gt=0, description="后段运行时限（模拟时钟单位）")


class ContinuationResult(BaseModel):
    accepted: bool
    front: JobView
    continuation: JobView
    events: list["EventView"]


class CompletionRequest(BaseModel):
    """作业完工回报。

    - ``at``: 回报对应的完工时刻，不得早于该作业实际开工时刻，
      不得晚于当前模拟时刻，也不得超过其运行时限。
    - 迟到的回报（节点已转配或作业已被时限杀掉）不会再次释放节点。
    """

    at: int = Field(gt=0, description="完工时刻（模拟时钟）")


class AdvanceRequest(BaseModel):
    steps: int = Field(gt=0, description="向前推进的模拟时钟单位数")


class RackOccupancy(BaseModel):
    rack: str
    capacity: int
    used: int
    free: int
    nodes: dict[int, int | None] = Field(
        description="节点号 -> 占用作业 id，空闲为 null"
    )


class ClusterStatus(BaseModel):
    now: int
    racks: list[RackOccupancy]
    running_jobs: list[JobView]
    queued_jobs: list[JobView]
    chained_jobs: list[JobView] = Field(
        default_factory=list,
        description="已受理、等待前段完工的接续后段",
    )
    recent_events: list["EventView"]


class EventView(BaseModel):
    time: int
    type: str
    job_id: int | None = None
    rack: str | None = None
    detail: str


class ScheduledReservation(BaseModel):
    job_id: int
    rack: str
    nodes: list[int]
    scheduled_start: int
    deadline: int = Field(description="预计释放时刻 = 开工 + 时限")


class AdvanceResult(BaseModel):
    from_time: int
    to_time: int
    events: list[EventView]
    status: ClusterStatus


class CompletionResult(BaseModel):
    accepted: bool
    job: JobView
    events: list[EventView]
    note: str | None = None


ClusterStatus.model_rebuild()
ContinuationResult.model_rebuild()
