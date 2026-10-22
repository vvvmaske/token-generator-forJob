from alembic import context
from sqlalchemy import create_engine
config = context.config
url = __import__('os').environ.get('DATABASE_URL', config.get_main_option('sqlalchemy.url')).replace('+asyncpg','')
def run_migrations_online():
    with create_engine(url).connect() as connection:
        context.configure(connection=connection, target_metadata=None)
        with context.begin_transaction(): context.run_migrations()
run_migrations_online()
