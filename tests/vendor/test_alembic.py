from alembic.config import Config
from alembic.script import ScriptDirectory

# Based on https://blog.jerrycodes.com/multiple-heads-in-alembic-migrations/


def test_only_single_head_revision_in_migrations():
    config = Config('alembic.ini')
    vma_script = ScriptDirectory(dir=config.get_section('vma')['script_location'])
    # digital_bookplate_script = ScriptDirectory(dir=config.get_section('digital_bookplates')['script_location'])

    # This will raise if there are multiple heads
    vma_script.get_current_head()
    # digital_bookplate_script.get_current_head()
