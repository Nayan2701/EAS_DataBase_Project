from google import genai
import os

# Ensure your key is set in the terminal before running this
# export GEMINI_API_KEY="your_key_here" 
api_key = "AIzaSyAquheHuFl2C8W1iEYCLx3BTuQMRpGv5Tk"

if not api_key:
    print("❌ Error: GEMINI_API_KEY not found in environment variables.")
else:
    print(f"✅ Found API Key: {api_key[:5]}...")
    try:
        client = genai.Client(api_key=api_key)
        print("\n--- Available Models ---")
        for m in client.models.list():
            if "generateContent" in m.supported_actions:
                print(f"Model: {m.name}")
    except Exception as e:
        print(f"Error listing models: {e}")