import os
from pandas.tseries.frequencies import to_offset

_D_CHUNK_FORMAT = {
    to_offset("AS"): os.path.join("%Y"),  # yearly
    to_offset("MS"): os.path.join("%Y", "%Y-%m"),  # monthly
    to_offset("D"): os.path.join("%Y", "%Y-%m", "%Y-%m-%d"),  # daily
    to_offset("H"): os.path.join("%Y", "%Y-%m", "%Y-%m-%d", "%Y-%m-%d_%H0000"),  # hourly
}

_DEFAULT_FREQ = "D"
_DEFAULT_CHUNK_FORMAT = _D_CHUNK_FORMAT[to_offset("AS")]


def _path_format(chunk):
    return _D_CHUNK_FORMAT[chunk]
    # try:
    #     return _D_CHUNK_FORMAT[chunk]
    # except KeyError:
    #     return _DEFAULT_CHUNK_FORMAT


def _sanitize_kind(kind):
    allowed = ['tick', 'bar']
    if kind not in allowed:
        raise NotImplementedError("kind is '%s' but must be in %s" % (kind, allowed))
    return kind


def _sanitize_freq(freq):
    if freq == '':
        freq = _DEFAULT_FREQ
    return to_offset(freq)


def _get_chunk(kind, freq, chunk):
    if chunk == '':
        if kind == 'tick':
            chunk = 'D'  # daily
        else:  # kind == 'bar'
            if freq.name in ['M', 'D']:  # monthly, daily
                chunk = 'AS'  # yearly start
            elif freq.name == 'H':  # hourly
                chunk = 'MS'  # monthly start
            elif freq.name == 'T':  # minutely
                if freq.n >= 30:
                    chunk = 'MS'  # monthly start
                else:
                    chunk = 'D'  # daily start
            else:  # ['S', 'L', 'U', 'N']  # secondly and less (or other)
                chunk = 'H'  # hourly
    return to_offset(chunk)


def data_directory(data_source, kind, start, freq='', chunk=''):
    kind = _sanitize_kind(kind)
    freq = _sanitize_freq(freq)
    chunk = _get_chunk(kind, freq, chunk)
    if kind != 'tick':
        fmt = os.path.join(data_source, kind, freq.freqstr, _path_format(chunk))
    else:
        fmt = os.path.join(data_source, kind, _path_format(chunk))
    return(start.strftime(fmt))


def chunks_bound(start, chunk, end=None):
    start_chunk = start
    end_chunk = start_chunk + chunk
    while(end_chunk < end or end is None):
        end_chunk = start_chunk + chunk
        yield(start_chunk, end_chunk)
        start_chunk += chunk
