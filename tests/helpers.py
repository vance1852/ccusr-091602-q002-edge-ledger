"""测试共用构造工具。"""


def make_msg(seq, *, device="devA", metric="temp", value=None, device_ts=None,
             gateway="gw1", recv_ts=None, gen=0, cursor=None, t0=1000.0):
    from app.model import RawMessage
    return RawMessage(
        device_id=device, seq=seq, metric=metric,
        value=float(seq if value is None else value),
        device_ts=(t0 + seq) if device_ts is None else device_ts,
        gateway_id=gateway,
        recv_ts=(t0 + seq) if recv_ts is None else recv_ts,
        gen=gen, source_cursor=seq if cursor is None else cursor,
    )
