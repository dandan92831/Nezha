import pandas as pd
import numpy as np
from log_parsing import *
from alarm import *
import concurrent.futures


class Trace(object):
    def __init__(self, traceid):
        self.traceid = traceid
        self.spans = []

    def sort_spans(self):
        self.spans.sort(key=lambda k: (k.events[0].timestamp, 0))

    def append_spans(self, span):
        self.spans.append(span)


class Span(object):
    def __init__(self, spanid, parentid, pod):
        self.spanid = spanid
        self.parentid = parentid
        self.pod = pod
        self.events = []

    def sort_events(self):
        self.events.sort(key=lambda k: (k.timestamp, 0))

    def append_event(self, event):
        self.events.append(event)


class Event(object):
    def __init__(self, event, pod, ns, timestamp=None, spanid=None, parentid=None):
        self.event = event
        self.pod = pod
        self.timestamp = timestamp
        self.spanid = spanid
        self.parentid = parentid
        self.ns = ns

    def show_event(self):
        logger.info(
            "%s, %s, %s, %s",
            self.timestamp,
            self.spanid,
            self.event,
            from_id_to_template(self.event, self.ns),
        )


class EventGraph:
    def __init__(self, log_template_miner):
        self.adjacency_list = {}
        self.node_list = set()
        self.pair_set = set()
        self.support_dict = {}
        self.log_template_miner = log_template_miner

    def add_edge(self, node1, node2):
        if node1 not in self.adjacency_list.keys():
            self.adjacency_list[node1] = []
        self.adjacency_list[node1].append(node2)
        self.node_list.add(node1.event)
        self.node_list.add(node2.event)

    def get_deepth_pod(self, traget_event):
        pod = ""
        deepth = 0
        while True:
            flag = False
            for key in self.adjacency_list.keys():
                for item in self.adjacency_list[key]:
                    if traget_event == item.event:
                        traget_event = key.event
                        if deepth == 0:
                            pod = item.pod
                        flag = True
                        if "start" in from_id_to_template(
                            key.event, self.log_template_miner
                        ) and "TraceID" not in from_id_to_template(
                            key.event, self.log_template_miner
                        ):
                            deepth = deepth + 1
                        break
                if flag == True:
                    break
            if flag == False:
                break
        return deepth, pod

    def get_support(self):
        for key in self.adjacency_list.keys():
            for item in self.adjacency_list[key]:
                supprot_key = str(key.event) + "_" + str(item.event)
                if supprot_key not in self.pair_set:
                    self.support_dict[supprot_key] = 1
                    self.pair_set.add(supprot_key)
                else:
                    self.support_dict[supprot_key] += 1
        # logger.info("support_dict: %s" % self.support_dict)
        return self.support_dict


def get_events_within_trace(
    trace_reader, log_reader, trace_id, alarm_list, ns, log_template_miner
):
    """
    func get_events_within_trace: get all metric alarm, log, span within a trace and transform to event
    :parameter
        trace_read - pd.csv_read(tracefile)
        log_reader - pd.csv_read(logfile)
        trace_id   - only one trace is processed at a time
        alarm_list - [{'pod': 'cartservice-579f59597d-wc2lz', 'alarm': [{'metric_type': 'CpuUsageRate(%)', 'alarm_flag': True}]
    :return
        trace class with all event (events in the same span was order by timestamp)
    """
    # trace = {"traceid": trace_id, "spans": []}
    trace = Trace("StartTraceId is %s" % trace_id)
    # logger.info(trace_id)
    log_span_id_list = log_reader.index.tolist()
    try:
        # find all span within a trace
        spans = trace_reader.loc[
            [trace_id],
            [
                "SpanID",
                "ParentID",
                "PodName",
                "StartTimeUnixNano",
                "EndTimeUnixNano",
                "OperationName",
            ],
        ]
        # pdb.set_trace()
        # print(len(spans['SpanID']))
        if len(spans["SpanID"]) > 0:
            # process span independentlt and order by timestamp
            for span_index in range(len(spans["SpanID"])):
                # span event
                span_id = spans["SpanID"].iloc[span_index]
                parent_id = spans["ParentID"].iloc[span_index]
                pod = spans["PodName"].iloc[span_index]
                # Add opration event first
                span = Span(span_id, parent_id, pod)
                # record span start and end event

                service = pod.rsplit("-", 1)[0]
                service = service.rsplit("-", 1)[0]

                start_event = (
                    service + " " + spans["OperationName"].iloc[span_index] + " start"
                )
                end_event = (
                    service + " " + spans["OperationName"].iloc[span_index] + " end"
                )

                span.append_event(
                    Event(
                        timestamp=np.ceil(
                            spans["StartTimeUnixNano"].iloc[span_index]
                        ).astype(int),
                        event=log_parsing(
                            log=start_event,
                            pod=pod,
                            log_template_miner=log_template_miner,
                        ),
                        pod=pod,
                        ns=ns,
                        spanid=span_id,
                        parentid=parent_id,
                    )
                )

                end_timestamp = np.ceil(
                    spans["EndTimeUnixNano"].iloc[span_index]
                ).astype(int)

                # log event
                try:
                    # pdb.set_trace()
                    if span_id in log_span_id_list:
                        logs = log_reader.loc[[span_id], ["TimeUnixNano", "Log"]]

                        if len(logs["TimeUnixNano"]) > 0:
                            for log_index in range(len(logs["TimeUnixNano"])):
                                # Add log events
                                # logger.info(logs['Log'].iloc[log_index])
                                log = logs["Log"].iloc[log_index]
                                timestamp = np.ceil(
                                    logs["TimeUnixNano"].iloc[log_index]
                                ).astype(int)

                                # for log end after span
                                if timestamp - end_timestamp > 0:
                                    # logger.info(json.loads(log)['log'])
                                    end_timestamp = timestamp + 1

                                # if log_parsing(log=log, pod=pod) != histroy_id:
                                #     histroy_id = log_parsing(log=log, pod=pod)
                                span.append_event(
                                    Event(
                                        timestamp=timestamp,
                                        event=log_parsing(
                                            log=log,
                                            pod=pod,
                                            log_template_miner=log_template_miner,
                                        ),
                                        pod=pod,
                                        ns=ns,
                                        spanid=span_id,
                                        parentid=parent_id,
                                    )
                                )

                    # else:
                    # logger.debug("Spanid %s is not appear in log" % span_id)
                except Exception as e:
                    logger.error("Catch an exception:", e)
                    pass

                span.append_event(
                    Event(
                        timestamp=end_timestamp,
                        event=log_parsing(
                            log=end_event,
                            pod=pod,
                            log_template_miner=log_template_miner,
                        ),
                        pod=pod,
                        ns=ns,
                        spanid=span_id,
                        parentid=parent_id,
                    )
                )
                # alarm event
                # hipster
                if ns == "hipster":
                    if len(span.events) > 2:
                        # if len(span.events) >= 2:
                        # do not add alarm for client span
                        for i in range(len(alarm_list)):
                            alarm_dict = alarm_list[i]
                            if alarm_dict["pod"] == span.pod:
                                # logger.info(
                                #     "%s, %s", alarm_dict["pod"], alarm_dict["alarm"])
                                for index in range(len(alarm_dict["alarm"])):
                                    span.append_event(
                                        Event(
                                            timestamp=np.ceil(
                                                spans["StartTimeUnixNano"].iloc[
                                                    span_index
                                                ]
                                            ).astype(int)
                                            + index
                                            + 1,
                                            event=log_parsing(
                                                log=alarm_dict["alarm"][index][
                                                    "metric_type"
                                                ],
                                                pod="alarm",
                                                log_template_miner=log_template_miner,
                                            ),
                                            pod=pod,
                                            ns=ns,
                                            spanid=span_id,
                                            parentid=parent_id,
                                        )
                                    )
                                # only add alarm event for the first span group
                                # alarm_list.pop(i)
                                break
                elif ns == "ts":
                    for i in range(len(alarm_list)):
                        alarm_dict = alarm_list[i]
                        if alarm_dict["pod"] == span.pod:
                            # logger.info(
                            #     "%s, %s", alarm_dict["pod"], alarm_dict["alarm"])
                            for index in range(len(alarm_dict["alarm"])):
                                span.append_event(
                                    Event(
                                        timestamp=np.ceil(
                                            spans["StartTimeUnixNano"].iloc[span_index]
                                        ).astype(int)
                                        + index
                                        + 1,
                                        event=log_parsing(
                                            log=alarm_dict["alarm"][index][
                                                "metric_type"
                                            ],
                                            pod="alarm",
                                            log_template_miner=log_template_miner,
                                        ),
                                        pod=pod,
                                        ns=ns,
                                        spanid=span_id,
                                        parentid=parent_id,
                                    )
                                )
                            # only add alarm event for the first span group
                            # alarm_list.pop(i)
                            break

                # sort event by event timestamp
                span.sort_events()

                trace.append_spans(span)

            # sort span by span start timestamp
            trace.sort_spans()
    except Exception as e:
        pass
        # logger.error("Catch an exception: %s", e)

    return trace


def generate_event_graph(trace, log_template_miner):
    """
    func generate_event_graph: integrate events of different span to graph
    :parameter
        trace - trace including spans with all event from get_events_within_trace
    :return
        event_graph -
    """
    event_graph = EventGraph(log_template_miner)

    for span in trace.spans:
        # add edge in the span group
        for index in range(1, len(span.events)):
            event_graph.add_edge(span.events[index - 1], span.events[index])

    for span in trace.spans:
        for parent_span in trace.spans:
            if parent_span.spanid == span.parentid:
                if parent_span.pod == span.pod:
                    # if in the same pod, insert based on timestamp
                    start_timestamp = span.events[0].timestamp

                    for index in range(1, len(parent_span.events)):
                        if parent_span.events[index].timestamp > start_timestamp:
                            event_graph.add_edge(
                                parent_span.events[index - 1], span.events[0]
                            )
                            break

                else:
                    # if not in the same pod, insert after the first span of parent group
                    event_graph.add_edge(parent_span.events[0], span.events[0])
                break
    return event_graph


def data_integrate(
    trace_file, trace_id_file, log_file, alarm_list, ns, log_template_miner
):
    """
    func data_integrate: integrate multimodle data to event graph
    :parameter
        trace_file
        trace_id_file
        log_file
        alarm_list
        ns
    :return
        list of event graph
    """
    logger.info(alarm_list)
    trace_id_reader = pd.read_csv(
        trace_id_file, index_col=False, header=None, engine="c"
    )
    trace_reader = pd.read_csv(
        trace_file,
        index_col="TraceID",
        usecols=[
            "TraceID",
            "SpanID",
            "ParentID",
            "PodName",
            "StartTimeUnixNano",
            "EndTimeUnixNano",
            "OperationName",
        ],
        engine="c",
    )
    log_reader = pd.read_csv(
        log_file,
        index_col="SpanID",
        usecols=["TimeUnixNano", "SpanID", "Log"],
        engine="c",
    )
    log_sequences = []
    event_graphs = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=64) as executor1:
        futures1 = {
            executor1.submit(
                get_events_within_trace,
                trace_reader,
                log_reader,
                traceid,
                alarm_list,
                ns,
                log_template_miner,
            )
            for traceid in trace_id_reader[0]
        }

        for future1 in concurrent.futures.as_completed(futures1):
            trace = future1.result()
            if trace is not None:
                log_sequences.append(trace)
        executor1.shutdown()

    with concurrent.futures.ProcessPoolExecutor(max_workers=64) as executor2:
        futures2 = {
            executor2.submit(generate_event_graph, trace, log_template_miner)
            for trace in log_sequences
        }

        for future2 in concurrent.futures.as_completed(futures2):
            graph = future2.result()
            if graph is not None:
                event_graphs.append(graph)
        executor2.shutdown()

    for graph in event_graphs:
        graph.get_support()

    logger.info("Data Integrate Complete!")
    return event_graphs
