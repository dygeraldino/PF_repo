from prisma import Prisma

# Cliente Prisma singleton — se conecta al iniciar la app y se desconecta al cerrar
prisma_client = Prisma()

def get_prisma() -> Prisma:
    return prisma_client
