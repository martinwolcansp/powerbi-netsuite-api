# netsuite_queue.py
import asyncio
import time
import logging

logger = logging.getLogger("netsuite")

# Lock global para serializar todas las llamadas a NetSuite
_lock = asyncio.Lock()


class NetSuiteQueue:
    def __init__(self):
        self.lock = _lock

    async def run(self, job_name: str, coro):
        """
        Ejecuta la coro de manera secuencial.
        Espera hasta que termine la request anterior.
        """
        async with self.lock:
            logger.info(f"NETSUITE | {job_name} | START")
            start = time.monotonic()

            try:
                result = await coro()
                duration = round(time.monotonic() - start, 2)
                # Resumen final
                if isinstance(result, list):
                    total = len(result)
                elif isinstance(result, dict):
                    total = sum(len(v) if isinstance(v, list) else 1 for v in result.values())
                else:
                    total = 1
                logger.info(f"NETSUITE | {job_name} | OK | {duration}s | total={total}")
                return result

            except Exception as e:
                duration = round(time.monotonic() - start, 2)
                logger.error(f"NETSUITE | {job_name} | ERROR | {duration}s | {e}")
                raise


# Singleton
netsuite_queue = NetSuiteQueue()