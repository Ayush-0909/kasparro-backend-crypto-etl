def test_failure_example():
    try:
        int("not_a_number")
        assert False
    except ValueError:
        assert True
