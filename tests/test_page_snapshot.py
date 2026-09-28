from scripts.page_snapshot import parse_args


def test_summary_is_the_default():
    args = parse_args([])
    assert args.page is None
    assert args.wiki is None


def test_page_and_wiki_options():
    assert parse_args(["--page", "enwiki:Paris"]).page == "enwiki:Paris"
    args = parse_args(["--wiki", "dewiki", "--limit", "3"])
    assert (args.wiki, args.limit) == ("dewiki", 3)
