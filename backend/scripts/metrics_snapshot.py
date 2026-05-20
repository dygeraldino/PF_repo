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


async def _run() -> int:
    parser = argparse.ArgumentParser(description="Snapshot de metricas operativas (AS-IS/TO-BE).")
    parser.add_argument("--start", help="Inicio en ISO-8601 (ej: 2026-05-01T00:00:00)")
    parser.add_argument("--end", help="Fin en ISO-8601 (ej: 2026-05-31T23:59:59)")
    parser.add_argument("--user-id", help="Filtrar por requested_by_user_id")
    parser.add_argument("--output", help="Ruta de salida JSON (opcional)")
    args = parser.parse_args()

    start = _parse_datetime(args.start)
    end = _parse_datetime(args.end)

    prisma = Prisma()
    await prisma.connect()
    try:
        deployments = await prisma.deployment.find_many(where=_build_where(start, end, args.user_id))
        stats = calculate_deployment_stats(deployments)
        stats["period"] = {
            "start": start.isoformat() if start else None,
            "end": end.isoformat() if end else None,
        }
        stats["user_id"] = args.user_id

        payload = json.dumps(stats, ensure_ascii=True, indent=2)
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
