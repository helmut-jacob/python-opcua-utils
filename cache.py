import opcua
from datetime import datetime
from portion import interval
from functools import reduce
from pprint import pprint

def node_read_full_history(node, start, end):
    """
    Read _full_ history of a node.
    """
    while True:
        vals = node.read_raw_history(starttime=start, endtime=end)
        yield from vals
        if not vals:
            # no data anymore, no need to proceed
            break
        if start == vals[-1].SourceTimestamp:
            # we've read the last value twice, no need to proceed
            break
        if start > end:
            # we've read the bounding value after end already, no need to proceed
            break
        start = vals[-1].SourceTimestamp

class NodeHistoryCache(object):
    """
    Cache the history of an OPCUA node and answer history reads from the cache
    if possible. The caching is based on the assumption that a nodes history
    doesn't change once written.
    """

    # FIXME: Ideas wrt cache expiration:
    # Flush cache on
    # * client re-connect (we don't know if anything changed inbetween)
    # * AuditHistoryUpdateEventType  (Something in the history changes)
    # * Least recently used history data to not grow the cache indefinitely
    # * Expire history data after timeout -> Not sure why this would be relevant?

    def __init__(self, node):
        self.history = {} # interval based dict
        self.node = node

    def _history_available_interval(self):
        if not self.history:
            return interval.empty()
        return reduce(lambda a,b: a | b, self.history.keys())

    def _get_missing_intervals(self, start, end):
        requested_int = interval.open(start, end)
        return self._get_available_interval(start, end).complement().intersection(requested_int)

    def _get_available_interval(self, start, end):
        """
        Return an interval describing for which intervals
        inside the requested timerange data is available in
        our local cache.
        """
        requested_int = interval.open(start, end)
        avail_int = self._history_available_interval() 
        return avail_int.intersection(requested_int) 

    def _populate_cache(self, intervals):
        print("Need to populate cache for ", intervals)
        for atomic_int in intervals:
            # insert missing intervals in cache
            dvs = node_read_full_history(self.node, atomic_int.lower, atomic_int.upper)
            span = interval.open(atomic_int.lower, atomic_int.upper)
            self.history[span] = list(dvs)

    def _get_partial_dvs(self, start, end, dvs):
        # get all dvs from a datavalue list
        return filter(lambda x: x.SourceTimestamp >= start and x.SourceTimestamp <= end, dvs)

    def _dvs_from_cache(self, start, end):
        req_int = interval.open(start, end)
        for atomicint, dvs in self.history.items():
            intersect = atomicint.intersection(req_int)
            if intersect.empty:
                # nothing here for us
                continue

            yield from self._get_partial_dvs(intersect.lower, intersect.upper, dvs)

    def get_history(self, start, end):
        print("Read history", start, end)
        missing = self._get_missing_intervals(start, end)
        print("I'm missing data for the following intervals", missing)
        
        if not missing.empty:
            # at least some timeranges seem to be missing,
            # fill in the holes
            self._populate_cache(missing)
        else:
            print("Request can be satisfied from cache")

        return self._dvs_from_cache(start, end)


c=opcua.Client("opc.tcp://0.0.0.0:4840")
c.connect()
cache = NodeHistoryCache(c.get_node("ns=3;i=1000114"))
pprint(list(cache.get_history(datetime(2020, 2, 18, 9, 34, 0), datetime(2020, 2, 18, 9, 34, 10))))
pprint(list(cache.get_history(datetime(2020, 2, 18, 9, 34, 5), datetime(2020, 2, 18, 9, 34, 7))))
pprint(list(cache.get_history(datetime(2020, 2, 18, 9, 34, 5), datetime(2020, 2, 18, 9, 34, 15))))

c.disconnect()
