from datetime import datetime
import pandas as pd
from taipy.gui import Gui, Markdown
from analyzer.analyzer import ask

# Global variables for the app
upload_path = None
chat_history = []  # stores convo history
user_input = ""    # what the user types
data_preview = pd.DataFrame()

# Handles chat input and responses
def on_chat(state):
    if state.user_input.strip():
        q = state.user_input.strip()
        state.chat_history.append(("You", q))
        response = ask(q)  
        state.chat_history.append(("Bot", response))
        state.user_input = ""

# UI layout and markdown for the chatbot page
page = Markdown(f"""

# Monkeytype Data Chatbot

---

## Chat with Your Data
Ask natural language questions about your typing performance, accuracy, speed trends, and more.

<|layout|columns=2 1|gap=32px|style=layout_style|
<|{{user_input}}|input|label=Type your question here...|on_action=on_chat|style=input_style|>
<|Ask|button|on_action=on_chat|style=button_style|>
|>

**Chat History:**  
<|{{chat_history}}|chat|style=chat_style|>
""")

# Main entry point to run the app
if __name__ == "__main__":
    gui = Gui(page)
    gui.run(title="Monkeytype Chatbot", port=5000)
