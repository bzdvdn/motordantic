from motordantic.sync import force_sync


async def get_data() -> str:
    return 'test accepted'


def test_force_sync():
    result = force_sync(get_data)()
    assert result == 'test accepted'
