from rca.baseline.rca_agent.controller import control_loop
import os

class RCA_Agent:
    def __init__(self, agent_prompt, basic_prompt) -> None:

        self.ap = agent_prompt
        self.bp = basic_prompt

    def run(self, instruction, logger, max_step=25, max_turn=5, debug=False, uuid=None, temperature=0.0):
        logger.info(f"Objective: {instruction}")
        if debug:
            logger.info(f"[DEBUG] max_step={max_step}, max_turn={max_turn}")
        # 新增：如果传入uuid，则直接读取预处理csv
        if uuid is not None:
            import pandas as pd
            log_path = f'preprocessed_csv/log_{uuid}.csv'
            metric_path = f'preprocessed_csv/metric_{uuid}.csv'
            trace_path = f'preprocessed_csv/trace_{uuid}.csv'
            log_df = pd.read_csv(log_path) if os.path.exists(log_path) else None
            metric_df = pd.read_csv(metric_path) if os.path.exists(metric_path) else None
            trace_df = pd.read_csv(trace_path) if os.path.exists(trace_path) else None
            # 你可以在这里将log_df, metric_df, trace_df传递给后续推理链或prompt
            # 这里只是示例，实际推理链需根据你的主流程适配
            logger.info(f"Loaded preprocessed data for uuid={uuid}: log={log_df is not None}, metric={metric_df is not None}, trace={trace_df is not None}")
            # 这里可直接return或继续走原有推理链
        prediction, trajectory, prompt = control_loop(
            instruction, "", self.ap, self.bp, logger=logger, max_step=max_step, max_turn=max_turn, debug=debug, temperature=temperature
        )
        logger.info(f"Result: {prediction}")
        if debug:
            logger.info(f"[DEBUG] Trajectory: {trajectory}")
            logger.info(f"[DEBUG] Prompt: {prompt}")
        return prediction, trajectory, prompt