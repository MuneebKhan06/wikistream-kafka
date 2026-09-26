from scripts.top_pages import BUSIEST_MINUTES, TOP_PAGES, parse_args


def test_defaults_cover_the_last_hour():
    args = parse_args([])
    assert args.hours == 1.0
    assert args.wiki is None
    assert args.busiest is False


def test_wiki_filter_and_window_are_parsed():
    args = parse_args(["--wiki", "enwiki", "--hours", "6", "--limit", "3"])
    assert (args.wiki, args.hours, args.limit) == ("enwiki", 6.0, 3)


def test_queries_take_named_parameters_only():
    for query in (TOP_PAGES, BUSIEST_MINUTES):
        assert "%(hours)s" in query
        assert "%(wiki)s" in query
        assert "%(limit)s" in query
        assert "%s" not in query.replace("%(hours)s", "").replace(
            "%(wiki)s", ""
        ).replace("%(limit)s", "")


def test_busiest_flag_selects_the_other_query():
    assert parse_args(["--busiest"]).busiest is True
