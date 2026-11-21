"""API 层测试：Swagger 对应端点、错误码与并发提交。"""

from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture()
def client() -> TestClient:
    c = TestClient(app)
    c.post("/simulation/reset")
    return c


def test_root_and_openapi(client: TestClient):
    r = client.get("/")
    assert r.status_code == 200
    assert r.json()["service"] == "mpi-backfill-scheduler"
    spec = client.get("/openapi.json").json()
    assert "/jobs/submit" in spec["paths"]
    assert "/simulation/advance" in spec["paths"]


def test_full_backfill_flow_via_api(client: TestClient):
    # 占满两个机架
    r = client.post("/jobs/submit", json={"nodes": 4, "duration": 10})
    j1 = r.json()["job_id"]
    client.post("/jobs/submit", json={"nodes": 3, "duration": 10})
    # 队首等待 → 预约
    j3 = client.post("/jobs/submit", json={"nodes": 4, "duration": 10}).json()
    assert j3["scheduled_start"] == 10
    reservations = client.get("/reservations").json()
    assert reservations[0]["job_id"] == j3["job_id"]
    assert reservations[0]["deadline"] == 20
    # 回填
    j4 = client.post("/jobs/submit", json={"nodes": 1, "duration": 3}).json()
    assert j4["status"] == "running" and j4["rack"] == "r2"
    j5 = client.post("/jobs/submit", json={"nodes": 1, "duration": 99}).json()
    assert j5["status"] == "queued"

    status = client.get("/simulation/status").json()
    assert status["now"] == 0
    assert {r["rack"]: r["used"] for r in status["racks"]} == {"r1": 4, "r2": 4}

    # 时钟还在 0，回报未来时刻 t=3 → 409
    bad = client.post(f"/jobs/{j1}/completion", json={"at": 3})
    assert bad.status_code == 409
    client.post("/simulation/advance", json={"steps": 3})
    ok = client.post(f"/jobs/{j1}/completion", json={"at": 3})
    assert ok.status_code == 200
    # 队首提前于预约开工
    j3_after = client.get(f"/jobs/{j3['job_id']}").json()
    assert j3_after["status"] == "running"
    assert j3_after["actual_start"] == 3

    # 迟到回报：j1 已完工，再报一次 → 409
    stale = client.post(f"/jobs/{j1}/completion", json={"at": 3})
    assert stale.status_code == 409


def test_advance_and_occupancy_endpoint(client: TestClient):
    client.post("/jobs/submit", json={"nodes": 4, "duration": 2})
    client.post("/jobs/submit", json={"nodes": 4, "duration": 4})
    client.post("/jobs/submit", json={"nodes": 4, "duration": 10})
    r = client.post("/simulation/advance", json={"steps": 6})
    assert r.status_code == 200
    body = r.json()
    assert body["from_time"] == 0 and body["to_time"] == 6
    event_types = [e["type"] for e in body["events"]]
    assert event_types.count("job_timeout") >= 2
    occ = client.get("/racks/occupancy").json()
    assert {x["rack"]: x["free"] for x in occ} == {"r1": 0, "r2": 4}


def test_error_codes(client: TestClient):
    assert client.post("/jobs/submit", json={"nodes": 0, "duration": 1}).status_code == 422
    assert client.post("/jobs/submit", json={"nodes": 99, "duration": 1}).status_code == 422
    assert client.get("/jobs/9999").status_code == 404
    assert client.post("/simulation/advance", json={"steps": 0}).status_code == 422


def test_concurrent_submissions_never_share_nodes(client: TestClient):
    async def scenario() -> None:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://test") as ac:
            # 16 个 1 节点作业并发提交，集群只有 8 个节点
            coros = [
                ac.post("/jobs/submit", json={"nodes": 1, "duration": 10})
                for _ in range(16)
            ]
            responses = await asyncio.gather(*coros)
            assert all(r.status_code == 201 for r in responses)

    asyncio.run(scenario())

    body = client.get("/simulation/status").json()
    owners = [owner for rack in body["racks"]
              for owner in rack["nodes"].values() if owner is not None]
    assert len(owners) == 8                       # 恰好 8 个节点被占
    assert len(body["running_jobs"]) == 8
    assert len(body["queued_jobs"]) == 8
    assert len(owners) == len(set(owners))        # 节点归属唯一


# ----------------------------------------------------------------- 接续（后段）


def test_continuation_full_handoff_flow_via_api(client: TestClient):
    # 前段占满 r1；r2 用长作业占满，使新队首只能等 r1
    front = client.post("/jobs/submit", json={"nodes": 4, "duration": 10}).json()
    client.post("/jobs/submit", json={"nodes": 4, "duration": 99})
    # 受理接续后段
    r = client.post(f"/jobs/{front['job_id']}/continuation", json={"duration": 5})
    assert r.status_code == 201
    body = r.json()
    assert body["accepted"] is True
    cont = body["continuation"]
    assert cont["status"] == "queued"
    assert cont["front_id"] == front["job_id"]
    assert cont["rack"] == "r1" and cont["assigned_nodes"] == [0, 1, 2, 3]
    assert body["front"]["continuation_id"] == cont["job_id"]

    # 状态接口可见待开工后段；节点仍归前段
    status = client.get("/simulation/status").json()
    assert [j["job_id"] for j in status["chained_jobs"]] == [cont["job_id"]]
    occ = client.get("/racks/occupancy").json()
    assert set(occ[0]["nodes"].values()) == {front["job_id"]}

    # 后段挂接后，等 r1 的新队首预约按两段跑满估算（10 + 5 = 15）
    head = client.post("/jobs/submit", json={"nodes": 4, "duration": 4}).json()
    assert head["scheduled_start"] == 15

    # 推进到 t=3 并回报前段完工 -> 后段在当前时刻交接开工
    client.post("/simulation/advance", json={"steps": 3})
    comp = client.post(f"/jobs/{front['job_id']}/completion", json={"at": 3})
    assert comp.status_code == 200
    kinds = [e["type"] for e in comp.json()["events"]]
    assert "job_completed" in kinds and "job_started" in kinds

    front_after = client.get(f"/jobs/{front['job_id']}").json()
    cont_after = client.get(f"/jobs/{cont['job_id']}").json()
    assert front_after["status"] == "finished" and front_after["finish_time"] == 3
    assert cont_after["status"] == "running" and cont_after["actual_start"] == 3
    # 节点占用过户给后段
    occ = client.get("/racks/occupancy").json()
    assert set(occ[0]["nodes"].values()) == {cont["job_id"]}
    # 队首预约提前到后段释放时刻 t=8
    head_after = client.get(f"/jobs/{head['job_id']}").json()
    assert head_after["scheduled_start"] == 8


def test_continuation_cancelled_on_front_timeout_via_api(client: TestClient):
    # r2 用长作业占满，使队首只能等前段所在的 r1
    client.post("/jobs/submit", json={"nodes": 4, "duration": 99})
    front = client.post("/jobs/submit", json={"nodes": 4, "duration": 3}).json()
    cont = client.post(
        f"/jobs/{front['job_id']}/continuation", json={"duration": 5}
    ).json()["continuation"]
    head = client.post("/jobs/submit", json={"nodes": 2, "duration": 4}).json()
    assert head["scheduled_start"] == 8           # 3 + 5

    adv = client.post("/simulation/advance", json={"steps": 3}).json()
    kinds = [e["type"] for e in adv["events"]]
    assert "job_timeout" in kinds and "continuation_cancelled" in kinds

    cont_after = client.get(f"/jobs/{cont['job_id']}").json()
    assert cont_after["status"] == "cancelled"
    assert cont_after["finish_reason"] == "cancelled"
    head_after = client.get(f"/jobs/{head['job_id']}").json()
    assert head_after["status"] == "running" and head_after["actual_start"] == 3


def test_continuation_reservation_conflict_rejected_via_api(client: TestClient):
    # 单机架语义：用 CLUSTER_RACKS 默认 4,4，先占满两个机架再制造冲突
    j1 = client.post("/jobs/submit", json={"nodes": 4, "duration": 10}).json()
    j2 = client.post("/jobs/submit", json={"nodes": 4, "duration": 10}).json()
    head = client.post("/jobs/submit", json={"nodes": 4, "duration": 10}).json()
    assert head["scheduled_start"] == 10
    # 给 j1 挂 1 单位后段：r1 释放延至 11，但队首可换 r2 仍赶上 t=10 -> 受理
    ok = client.post(f"/jobs/{j1['job_id']}/continuation", json={"duration": 1})
    assert ok.status_code == 201
    # 再给 j2 挂 1 单位后段：r2 也延至 11，队首换不到任何机架 -> 拒绝
    conflict = client.post(f"/jobs/{j2['job_id']}/continuation",
                           json={"duration": 1})
    assert conflict.status_code == 409
    # 原作业与预约照旧
    j2_after = client.get(f"/jobs/{j2['job_id']}").json()
    assert j2_after["status"] == "running"
    assert j2_after["continuation_id"] is None
    reservations = client.get("/reservations").json()
    assert reservations[0]["job_id"] == head["job_id"]
    assert reservations[0]["scheduled_start"] == 10


def test_continuation_error_codes(client: TestClient):
    assert client.post("/jobs/9999/continuation",
                       json={"duration": 1}).status_code == 404
    # 占满两个机架，制造一个排队作业
    front = client.post("/jobs/submit", json={"nodes": 4, "duration": 10}).json()
    client.post("/jobs/submit", json={"nodes": 4, "duration": 10})
    queued = client.post("/jobs/submit", json={"nodes": 4, "duration": 10}).json()
    assert queued["status"] == "queued"
    # 未运行的作业不能挂接续
    assert client.post(f"/jobs/{queued['job_id']}/continuation",
                       json={"duration": 1}).status_code == 409
    # 时限必须为正整数
    assert client.post(f"/jobs/{front['job_id']}/continuation",
                       json={"duration": 0}).status_code == 422
    # 每个前段最多接一段
    assert client.post(f"/jobs/{front['job_id']}/continuation",
                       json={"duration": 5}).status_code == 201
    assert client.post(f"/jobs/{front['job_id']}/continuation",
                       json={"duration": 5}).status_code == 409
