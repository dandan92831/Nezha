import datetime
from data_integrate import *

log_path = (
    dirname(__file__)
    + "/log/"
    + str(datetime.datetime.now().strftime("%Y-%m-%d"))
    + "_nezha.log"
)
logger = Logger(log_path, logging.DEBUG, __name__).getlog()


def get_pattern_support(event_graphs):
    result_support_dict = {}
    total_pair = set()

    for event_graph in event_graphs:
        for key, value in event_graph.support_dict.items():
            if key in total_pair:
                result_support_dict[key] += value
            else:
                result_support_dict[key] = value
        total_pair = total_pair | event_graph.pair_set

    result_support_dict = dict(
        sorted(result_support_dict.items(), key=lambda x: x[1], reverse=True)
    )

    return result_support_dict
