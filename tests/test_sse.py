from ingestor.sse import SSEParser


def parse_all(text):
    parser = SSEParser()
    events = []
    for line in text.split("\n"):
        event = parser.feed(line)
        if event is not None:
            events.append(event)
    return events


def test_parses_event_with_id_and_data():
    events = parse_all(
        'event: message\nid: [{"offset":1}]\ndata: {"meta":{"id":"a"}}\n\n'
    )
    assert len(events) == 1
    assert events[0].event == "message"
    assert events[0].event_id == '[{"offset":1}]'
    assert events[0].data == '{"meta":{"id":"a"}}'


def test_multiline_data_is_joined():
    events = parse_all("data: line one\ndata: line two\n\n")
    assert events[0].data == "line one\nline two"


def test_comments_and_blank_frames_are_ignored():
    events = parse_all(": keep alive\n\n:another\n\ndata: real\n\n")
    assert len(events) == 1
    assert events[0].data == "real"


def test_incomplete_event_is_not_emitted():
    parser = SSEParser()
    assert parser.feed("data: half") is None
    assert parser.data_lines == ["half"]


def test_event_type_resets_between_events():
    events = parse_all("event: error\ndata: boom\n\ndata: normal\n\n")
    assert [e.event for e in events] == ["error", "message"]


def test_value_without_leading_space():
    events = parse_all("data:tight\n\n")
    assert events[0].data == "tight"
