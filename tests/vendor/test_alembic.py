from alembic.config import Config
from alembic.script import ScriptDirectory

# Based on https://blog.jerrycodes.com/multiple-heads-in-alembic-migrations/


def test_only_single_head_revision_in_migrations():
    config = Config('alembic.ini')
    script = ScriptDirectory.from_config(config)

    # This will raise if there are multiple heads
    script.get_current_head()
