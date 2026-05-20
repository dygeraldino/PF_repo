import argparse
import asyncio
import json
from datetime import datetime, timezone

from prisma import Prisma

from app.services.deployment_service import calculate_deployment_stats


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _build_where(start: datetime | None, end: datetime | None, user_id: str | None) -> dict:
    where: dict = {}
    if user_id:
        where["requested_by_user_id"] = user_id
    if start or end:
        where["requested_at"] = {}
        if start:
            where["requested_at"]["gte"] = start
        if end:
            where["requested_at"]["lte"] = end
    return where


def _delta(base: float, target: float) -> dict:
    diff = target - base
    pct = None
    if base != 0:
        pct = round((diff / base) * 100, 2)
    return {"delta": round(diff, 2), "percent_change": pct}


async def _run() -> int:
    parser = argparse.ArgumentParser(description="Compara metricas AS-IS vs TO-BE.")
    parser.add_argument("--as-is-start", required=True, help="Inicio AS-IS (ISO-8601)")
    parser.add_argument("--as-is-end", required=True, help="Fin AS-IS (ISO-8601)")
    parser.add_argument("--to-be-start", required=True, help="Inicio TO-BE (ISO-8601)")
    parser.add_argument("--to-be-end", required=True, help="Fin TO-BE (ISO-8601)")
    parser.add_argument("--user-id", help="Filtrar por requested_by_user_id")
    parser.add_argument("--output", help="Ruta de salida JSON (opcional)")
    args = parser.parse_args()

    as_is_start = _parse_datetime(args.as_is_start)
    as_is_end = _parse_datetime(args.as_is_end)
    to_be_start = _parse_datetime(args.to_be_start)
    to_be_end = _parse_datetime(args.to_be_end)

    prisma = Prisma()
    await prisma.connect()
    try:
        as_is_deps = await prisma.deployment.find_many(
            where=_build_where(as_is_start, as_is_end, args.user_id)
        )
        to_be_deps = await prisma.deployment.find_many(
            where=_build_where(to_be_start, to_be_end, args.user_id)
        )

        as_is_stats = calculate_deployment_stats(as_is_deps)
        to_be_stats = calculate_deployment_stats(to_be_deps)

        comparison = {
            "as_is": {
                **as_is_stats,
                "period": {"start": as_is_start.isoformat(), "end": as_is_end.isoformat()},
            },
            "to_be": {
                **to_be_stats,
                "period": {"start": to_be_start.isoformat(), "end": to_be_end.isoformat()},
            },
            "delta": {
                "total_deployments": _delta(as_is_stats["total_deployments"], to_be_stats["total_deployments"]),
                "success_rate": _delta(as_is_stats["success_rate"], to_be_stats["success_rate"]),
                "avg_duration_seconds": _delta(as_is_stats["avg_duration_seconds"], to_be_stats["avg_duration_seconds"]),
                "rollback_count": _delta(as_is_stats["rollback_count"], to_be_stats["rollback_count"]),
                "mttr_minutes": _delta(as_is_stats["mttr_minutes"], to_be_stats["mttr_minutes"]),
                "concurrent_deploys": _delta(as_is_stats["concurrent_deploys"], to_be_stats["concurrent_deploys"]),
            },
        }

        payload = json.dumps(comparison, ensure_ascii=True, indent=2)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as handle:
                handle.write(payload)
        else:
            print(payload)
    finally:
        await prisma.disconnect()

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_run()))
