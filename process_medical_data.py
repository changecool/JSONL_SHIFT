import json

# 用于过滤掉仅用于标记属性的关系类型
ATTRIBUTE_REL_TYPES = {"药物用量", "炮制方法", "药物功用", "方剂用法", "方剂剂型"}

def process_medical_record(record_json, global_patient=None, id_counters=None):
    """
    处理单条医案数据（来自 JSONL 文件一行），输出嵌套后的诊疗事件结构，
    同时保留医案 id 与原文。  
    新增的“方剂”实体 id 格式为 "{医案id}-{事件order}-{当前事件新增方剂序号}"，
    名称为该 id 后附加“号方”。  
    
    参数：
      record_json: str，单条医案的 JSON 字符串（包含 "id", "text", "entities", "relations", "Comments"）
      global_patient: 若前面已有“患者”实体，则后续事件继承；初次传入 None
      id_counters: dict，包含全局最大 id（仅针对新增的非“方剂”实体），格式如 {"entity": max_id, "relation": max_id}
      
    返回：
      processed: dict，格式为 { "id": ..., "text": ..., "事件": [ ... ] }
      patient: 本医案中的“患者”实体（用于后续事件继承）
      id_counters: 更新后的 id_counters
    """
    data = json.loads(record_json)
    record_id = data.get("id")
    text = data.get("text", "")
    entities = data.get("entities", [])
    relations = data.get("relations", [])
    
    # 建立实体 id 到实体的映射
    entity_by_id = {e["id"]: e for e in entities}
    
    # 如果没有提供 id_counters，则初始化，取当前所有实体和关系中最大的数值（仅考虑整数 id）
    if id_counters is None:
        max_ent = max([e["id"] for e in entities if isinstance(e["id"], int)], default=0)
        max_rel = max([r["id"] for r in relations if isinstance(r["id"], int)], default=0)
        id_counters = {"entity": max_ent, "relation": max_rel}
    
    # -------------------------------
    # 1. 合并“中药”与“辅料”的属性（用量、制法、功用）
    for rel in relations:
        rel_type = rel.get("type")
        if rel_type in {"药物用量", "炮制方法", "药物功用"}:
            from_ent = entity_by_id.get(rel.get("from_id"))
            to_ent   = entity_by_id.get(rel.get("to_id"))
            if not from_ent or not to_ent:
                continue
            if from_ent.get("label") in ["中药", "辅料"]:
                if rel_type == "药物用量":
                    prop_key = "用量"
                elif rel_type == "炮制方法":
                    prop_key = "制法"
                elif rel_type == "药物功用":
                    prop_key = "功用"
                else:
                    prop_key = rel_type
                if "属性" not in from_ent:
                    from_ent["属性"] = {}
                from_ent["属性"][prop_key] = to_ent
    # -------------------------------
    # 2. 合并“患者”属性（年龄、性别）
    patient = None
    for e in entities:
        if e.get("label") == "患者":
            patient = e
            break
    if not patient and global_patient:
        patient = global_patient
    if patient:
        for e in entities:
            if e.get("label") in ["年龄", "性别"]:
                if "属性" not in patient:
                    patient["属性"] = {}
                patient["属性"][e["label"]] = e
    # -------------------------------
    # 3. 划分诊疗事件范围
    # 规则：从文本开始到下一个 type 为“诊疗事件触发词”的实体之间为一个事件；若无触发词，则全文为一事件
    trigger_entities = sorted(
        [e for e in entities if e.get("label") == "诊疗事件触发词"],
        key=lambda x: x.get("start_offset", 0)
    )
    event_boundaries = []
    if not trigger_entities:
        event_boundaries.append((0, len(text)))
    else:
        event_boundaries.append((0, trigger_entities[0]["start_offset"]))
        for i in range(len(trigger_entities)-1):
            event_boundaries.append((trigger_entities[i]["start_offset"], trigger_entities[i+1]["start_offset"]))
        event_boundaries.append((trigger_entities[-1]["start_offset"], len(text)))
    # -------------------------------
    # 4. 针对每个事件内部处理（方剂、中药、辅料、论元划分）
    events = []
    # 对于每个事件内新增的“方剂”实体，使用单独的序号（从1开始）
    for idx, (start, end) in enumerate(event_boundaries):
        event_order = idx + 1
        new_fangji_seq = 1  # 当前事件内新增方剂的序号计数器
        
        # 选取事件范围内的实体：实体的 start_offset 与 end_offset 均在该范围内
        event_entities = [e for e in entities if e.get("start_offset", 0) >= start and e.get("end_offset", 0) <= end]
        # 选取事件范围内的关系：要求关系两端实体均在该范围内
        event_relations = [r for r in relations if
                           (entity_by_id.get(r.get("from_id"), {}).get("start_offset", 0) >= start and
                            entity_by_id.get(r.get("from_id"), {}).get("end_offset", 0) <= end) and
                           (entity_by_id.get(r.get("to_id"), {}).get("start_offset", 0) >= start and
                            entity_by_id.get(r.get("to_id"), {}).get("end_offset", 0) <= end)]
        
        # 4.1 对已标注的“方剂”实体，补充“用法”与“剂型”属性（此处属性关系已在标注中存在）
        fangji_list = [e for e in event_entities if e.get("label") == "方剂"]
        for fj in fangji_list:
            for rel in relations:
                if rel.get("from_id") == fj["id"] and rel.get("type") in {"方剂用法", "方剂剂型"}:
                    to_ent = entity_by_id.get(rel.get("to_id"))
                    if not to_ent:
                        continue
                    prop_key = "用法" if rel.get("type") == "方剂用法" else "剂型"
                    if "属性" not in fj:
                        fj["属性"] = {}
                    fj["属性"][prop_key] = to_ent
        
        # 4.2 处理事件内的“未知方剂”
        unknown_fj_list = [e for e in event_entities if e.get("label") == "未知方剂"]
        for unk in unknown_fj_list:
            # 根据“未知方剂”的文本范围，提取该范围内（除自身外）的其他实体
            unk_start = unk.get("start_offset", 0)
            unk_end = unk.get("end_offset", 0)
            nested = [e for e in event_entities if e["id"] != unk["id"] and
                      e.get("start_offset", 0) >= unk_start and e.get("end_offset", 0) <= unk_end]
            # 收集中药、辅料，以及用法、剂型属性
            nested_zhongyao = []
            nested_fuliao = []
            fangji_attr = {}
            for sub in nested:
                sub_label = sub.get("label")
                if sub_label == "中药":
                    nested_zhongyao.append(sub)
                elif sub_label == "辅料":
                    nested_fuliao.append(sub)
                elif sub_label in ["用法", "剂型"]:
                    fangji_attr[sub_label] = sub
            # 从事件实体列表中删除原“未知方剂”
            event_entities = [e for e in event_entities if e["id"] != unk["id"]]
            # 新增规则命名的“方剂”实体
            new_fangji_id = f"{record_id}-{event_order}-{new_fangji_seq}"
            new_fangji_seq += 1
            new_fangji = {
                "id": new_fangji_id,
                "label": "方剂",
                "text": f"{new_fangji_id}号方",
                "属性": fangji_attr
            }
            event_entities.append(new_fangji)
            # 将新增的“方剂”实体更新到实体映射中
            entity_by_id[new_fangji_id] = new_fangji
            # 建立组成关系：中药→方剂 和 辅料→方剂
            for zy in nested_zhongyao:
                if zy not in event_entities:
                    event_entities.append(zy)
                id_counters["relation"] += 1
                rel_obj = {
                    "id": id_counters["relation"],
                    "from_id": zy["id"],
                    "to_id": new_fangji["id"],
                    "type": "组成"
                }
                event_relations.append(rel_obj)
            for fl in nested_fuliao:
                if fl not in event_entities:
                    event_entities.append(fl)
                id_counters["relation"] += 1
                rel_obj = {
                    "id": id_counters["relation"],
                    "from_id": fl["id"],
                    "to_id": new_fangji["id"],
                    "type": "作为辅料组成"
                }
                event_relations.append(rel_obj)
        
        # 4.3 对关系进行过滤：剔除仅用于属性标记的关系
        event_relations = [r for r in event_relations if r.get("type") not in ATTRIBUTE_REL_TYPES]
        
        # 4.4 划分论元
        # 辨证论元：包括 "患者"、"症状"、"病因病机"
        bianzhen_entities = [e for e in event_entities if e.get("label") in ["患者", "症状", "病因病机"]]
        if not any(e.get("label") == "患者" for e in bianzhen_entities) and patient:
            bianzhen_entities.append(patient)
        # 论治论元：包括 "治则治法"、"方剂"、"中药"、"辅料"
        lunzhi_entities = [e for e in event_entities if e.get("label") in ["治则治法", "方剂", "中药", "辅料"]]
        # 此处直接采用过滤后的 event_relations 作为论治论元的关系
        lunzhi_relations = event_relations
        # 理论依据论元：包括 "引文"
        lilun_entities = [e for e in event_entities if e.get("label") == "引文"]
        
        # 4.5 构造事件对象，记录事件编号、文本范围、原文及各论元
        event_obj = {
            "event_id": f"Event_{event_order}",
            "order": event_order,
            "text_range": [start, end],
            "原文": text[start:end],
            "辨证论元": {
                "entities": bianzhen_entities,
                "relations": []  # 如需要可进一步构造内部关系
            },
            "论治论元": {
                "entities": lunzhi_entities,
                "relations": lunzhi_relations
            },
            "理论依据论元": {
                "entities": lilun_entities,
                "relations": []  # 如需要可补充
            }
        }
        events.append(event_obj)
    # -------------------------------
    # 最终输出结果保留原医案 id 与 text
    processed = {"id": record_id, "text": text, "事件": events}
    return processed, patient, id_counters


def process_jsonl_file(input_path, output_path):
    """
    逐行读取输入 JSONL 文件（每行一条医案），执行预处理后输出嵌套后的 JSONL 文件。
    """
    output_lines = []
    global_id_counters = {"entity": 0, "relation": 0}
    global_patient = None
    with open(input_path, "r", encoding="utf-8") as fin:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            processed, patient, global_id_counters = process_medical_record(line, global_patient, global_id_counters)
            if patient:
                global_patient = patient  # 初诊事件的患者信息后续事件继承
            output_lines.append(json.dumps(processed, ensure_ascii=False))
    with open(output_path, "w", encoding="utf-8") as fout:
        for line in output_lines:
            fout.write(line + "\n")


if __name__ == "__main__":
    # 示例运行：输入文件 input.jsonl，输出文件 output_processed.jsonl
    input_file = "input.jsonl"
    output_file = "output_processed.jsonl"
    process_jsonl_file(input_file, output_file)
    print("处理完成，结果已写入", output_file)
