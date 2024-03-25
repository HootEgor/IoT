from app.entities.agent_data import AgentData
from app.entities.processed_agent_data import ProcessedAgentData


def process_agent_data(agent_data: AgentData) -> ProcessedAgentData:
    road_state = classify_road_state(agent_data.accelerometer.z)
    return ProcessedAgentData(road_state=road_state, agent_data=agent_data)

def classify_road_state(z_coordinate: float) -> str:
    if z_coordinate < -9.8:
        return "Excellent"
    elif -9.8 <= z_coordinate < -7.5:
        return "Good"
    elif -7.5 <= z_coordinate < -5.0:
        return "Fair"
    elif -5.0 <= z_coordinate < -2.5:
        return "Poor"
    else:
        return "Bad"
