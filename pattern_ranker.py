import datetime
from data_integrate import (
    get_metric_with_time,
    generate_alarm,
    data_integrate,
    from_id_to_template,
)
from pattern_miner import get_pattern_support
from log import Logger
import logging
import json


def init_logger():
    log_path = (
        "./log/" + str(datetime.datetime.now().strftime("%Y-%m-%d")) + "_nezha.log"
    )
    logger = Logger(log_path, logging.DEBUG, __name__).getlog()
    return logger


logger = init_logger()


def get_pattern(detete_time, ns, data_path, log_template_miner, topk=30):
    """
    func get_pattern: get pattern at the detete_time
    :parameter
        detete_time  - query data
        data_path  - base data path
        topk - return topk pattern
    :return
        pattern_list
        event_graphs
    """
    date = detete_time.split(" ")[0]
    hour = detete_time.split(" ")[1].split(":")[0]
    min = detete_time.split(" ")[1].split(":")[1]

    trace_file = (
        data_path + "/" + date + "/trace/" + str(hour) + "_" + str(min) + "_trace.csv"
    )
    trace_id_file = (
        data_path
        + "/"
        + date
        + "/traceid/"
        + str(hour)
        + "_"
        + str(min)
        + "_traceid.csv"
    )
    log_file = (
        data_path + "/" + date + "/log/" + str(hour) + "_" + str(min) + "_log.csv"
    )

    metric_list = get_metric_with_time(detete_time, data_path)
    alarm_list = generate_alarm(metric_list, ns)
    event_graphs = data_integrate(
        trace_file, trace_id_file, log_file, alarm_list, ns, log_template_miner
    )
    result_support_list = get_pattern_support(event_graphs)

    return result_support_list, event_graphs, alarm_list


def get_event_depth_pod(normal_event_graphs, event_pair):
    source = int(event_pair.split("_")[0])
    maxdepth = 0
    event_pod = ""

    for normal_event_graph in normal_event_graphs:
        deepth, pod = normal_event_graph.get_deepth_pod(source)
        if deepth > maxdepth:
            maxdepth = deepth
            event_pod = pod

    return maxdepth, event_pod


def abnormal_pattern_ranker(normal_pattern_dict, abnormal_pattern_dict, min_score=0.67):
    score_dict = {}
    for key in abnormal_pattern_dict.keys():
        if abnormal_pattern_dict[key] > 5:
            if key not in score_dict.keys():
                score_dict[key] = 0
            if key in normal_pattern_dict.keys():
                score_dict[key] = (
                    1.0
                    * abnormal_pattern_dict[key]
                    / (abnormal_pattern_dict[key] + normal_pattern_dict[key])
                )
            else:
                score_dict[key] = 1.0

    move_list = set()
    for key, value in score_dict.items():
        if float(value) < min_score:
            move_list.add(key)
    for item in move_list:
        score_dict.pop(item)

    score_dict = sorted(score_dict, reverse=True)

    return score_dict


def pattern_ranker(
    normal_pattern_dict,
    normal_event_graphs,
    abnormal_time,
    ns,
    log_template_miner,
    rca_path,
    topk=10,
    min_score=0.67,
):
    abnormal_pattern_dict, _, alarm_list = get_pattern(
        abnormal_time, ns, rca_path, log_template_miner
    )
    abnormal_pattern_score = abnormal_pattern_ranker(
        normal_pattern_dict, abnormal_pattern_dict, min_score
    )
    score_dict = {}
    for key in normal_pattern_dict.keys():
        if normal_pattern_dict[key] > 5:
            if key not in score_dict.keys():
                score_dict[key] = 0
            if key in abnormal_pattern_dict.keys():
                score_dict[key] = (
                    1.0
                    * normal_pattern_dict[key]
                    / (abnormal_pattern_dict[key] + normal_pattern_dict[key])
                )
            else:
                score_dict[key] = 1.0

    move_list = set()
    for key, value in score_dict.items():
        if float(value) < min_score:
            move_list.add(key)
    for item in move_list:
        score_dict.pop(item)

    move_list = set()
    for key in score_dict.keys():
        if (
            "Cpu" not in from_id_to_template(int(key.split("_")[1]), log_template_miner)
            and "Network"
            not in from_id_to_template(int(key.split("_")[1]), log_template_miner)
            and "Memory"
            not in from_id_to_template(int(key.split("_")[1]), log_template_miner)
        ):
            for key1 in score_dict.keys():
                if (
                    int(key.split("_")[0]) == int(key1.split("_")[1])
                    and score_dict[key] <= score_dict[key1]
                ):
                    move_list.add(key)
    for item in move_list:
        score_dict.pop(item)

    result_list = []
    deepth_dict = {}
    for key, value in score_dict.items():
        deepth, pod = get_event_depth_pod(normal_event_graphs, key)
        if pod not in deepth_dict:
            deepth_dict[pod] = deepth
        elif deepth_dict[pod] < deepth:
            deepth_dict[pod] = deepth

        if pod == "":
            pod = "frontend-579b9bff58-t2dbm"
            deepth = 1
        alarm_flag = False
        if len(alarm_list) > 0:
            for i in range(len(alarm_list)):
                item = alarm_list[i]
                if item["pod"] == pod:
                    result_list.append(
                        {
                            "events": key,
                            "score": value,
                            "deepth": deepth,
                            "pod": pod,
                            "resource": item["alarm"][0]["metric_type"],
                        }
                    )
                    alarm_flag = True
                    break
        if not alarm_flag:
            result_list.append(
                {"events": key, "score": value, "deepth": deepth, "pod": pod}
            )

    move_list = set()
    for item in alarm_list:
        if item["pod"] in deepth_dict.keys():
            max_deep = deepth_dict[item["pod"]]
            mv_flag = False
            for i in range(len(result_list)):
                item1 = result_list[i]
                if "resource" in item1.keys():
                    if (
                        item1["pod"] == item["pod"]
                        and item1["resource"] == item["alarm"][0]["metric_type"]
                    ):
                        if max_deep > item1["deepth"]:
                            move_list.add(i)
                        elif max_deep == item1["deepth"] and mv_flag:
                            move_list.add(i)
                        else:
                            mv_flag = True

    move_list = list(move_list)
    move_list.reverse()
    try:
        for item in move_list:
            result_list.pop(item)
    except Exception as e:
        logger.error("Catch an exception: %s", e)

    result_list = sorted(
        result_list, key=lambda i: (i["score"], i["deepth"]), reverse=True
    )

    logger.info("Sorted Result List: %s" % result_list)

    return result_list, abnormal_pattern_score


def evaluation_pod(
    normal_time_list,
    fault_inject_list,
    ns,
    log_template_miner,
    construction_data_path="./construct_data",
):
    """
    func evaluation: evaluate nezha's precision in pod-service level
    para:
    - normal_time_list:  list of normal construction time
    - fault_inject_list: list of ground truth
    - ns: namespace of microservice
    return:
    nezha's precision
    """
    fault_number = 0
    top_list = []

    for i in range(len(fault_inject_list)):
        ground_truth_path = fault_inject_list[i]
        normal_time = normal_time_list[i]
        
        normal_pattern_list, normal_event_graphs, _ = get_pattern(normal_time, ns, construction_data_path, log_template_miner)
        with open(ground_truth_path) as f:
            fault_inject_data = json.load(f)

        root_cause_file = construction_data_path + "/root_cause_" + ns + ".json"

        with open(root_cause_file) as root_cause_lit_file:
            root_cause_list = json.load(root_cause_lit_file)

        for hour in fault_inject_data:
            for fault in fault_inject_data[hour]:
                fault_number = fault_number + 1
                abnormal_time = '2022-08-23 12:03'
                result_list, abnormal_pattern_score = pattern_ranker(
                    normal_pattern_list,
                    normal_event_graphs,
                    abnormal_time,
                    ns,
                    log_template_miner,
                    rca_path="./rca_data",
                )
                from pprint import pprint

                pprint(result_list)
                exit(0)
        
        # todo: compare with root_cause_list ground truth
