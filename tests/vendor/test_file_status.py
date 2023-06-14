from libsys_airflow.plugins.vendor.models import FileStatus


def test_can_set_loaded():
    assert FileStatus.not_fetched.can_set_loaded() is True
    assert FileStatus.fetched.can_set_loaded() is True
    assert FileStatus.uploaded.can_set_loaded() is True
    assert FileStatus.skipped.can_set_loaded() is True
    assert FileStatus.loading_error.can_set_loaded() is True


def test_cant_set_loaded():
    assert FileStatus.loading.can_set_loaded() is False
    assert FileStatus.loaded.can_set_loaded() is False
    assert FileStatus.purged.can_set_loaded() is False
