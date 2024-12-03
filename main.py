from pattern_ranker import evaluation_pod, Logger, logging
import argparse
from log_parsing import TemplateMiner, FilePersistence, TemplateMinerConfig

log_path = "./log/nezha.log"
logger = Logger(log_path, logging.DEBUG, __name__).getlog()


def get_miner(ns):
    config = TemplateMinerConfig()
    config.load("." + "/log_template/drain3_" + ns + ".ini")
    path = "." + "/log_template/" + ns + ".bin"
    persistence = FilePersistence(path)
    template_miner = TemplateMiner(persistence, config=config)

    return template_miner


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nezha")

    parser.add_argument("--ns", default="hipster", help="namespace")
    parser.add_argument(
        "--level", default="service", help="service-level or inner-service level"
    )

    args = parser.parse_args()
    ns = args.ns
    level = args.level

    normal_time2 = "2022-08-23 17:00"
    path2 = "./rca_data/2022-08-23/2022-08-23-fault_list.json"

    log_template_miner = get_miner(ns)
    inject_list = [path2]  # 指定异常时间
    normal_time_list = [normal_time2]  # 指定正常时间
    evaluation_pod(normal_time_list, inject_list, ns, log_template_miner)
