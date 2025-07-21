from rca.baseline.rca_agent.controller import control_loop
import os
import json
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
            meta_file = f'dataset/phaseone/processed_data/problems_data/3/problem_{uuid}/metadata.json'
            with open(meta_file, 'r') as f:
                meta_data = json.load(f)

            # 你可以在这里将log_df, metric_df, trace_df传递给后续推理链或prompt
            # 这里只是示例，实际推理链需根据你的主流程适配
            logger.info(f"Loaded preprocessed data for uuid={uuid}: log={'log_data' in meta_data['data_stats']}, metric={'metric_data' in meta_data['data_stats']}, trace={'trace_data' in meta_data['data_stats']}")
            # 这里可直接return或继续走原有推理链
            cand = self.bp.cand
            schema = self.bp.schema + uuid
        prediction, trajectory, prompt = control_loop(
            instruction, "", self.ap, schema, cand, logger=logger, max_step=max_step, max_turn=max_turn, debug=debug, temperature=temperature
        )
        logger.info(f"Result: {prediction}")
        if debug:
            logger.info(f"[DEBUG] Trajectory: {trajectory}")
            logger.info(f"[DEBUG] Prompt: {prompt}")
        return prediction, trajectory, prompt