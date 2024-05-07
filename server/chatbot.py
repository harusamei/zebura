import nltk
from nltk.chat.util import Chat, reflections

# Define custom functions
def my_custom_function(input):
    # Add your custom logic here
    return input+"This is the output of my custom function."

# Define chat pairs
pairs = [
    [
        r"call my custom function",
        ['my_custom_function']
    ],
    [
        r"exit",
        ["Goodbye!"]
    ]
]

# Create chatbot
chatbot = Chat(pairs, reflections)

# Start the conversation
while True:
    user_input = input("> ")
    if user_input == "exit":
        print("Goodbye!")
        break
    else:
        response = chatbot.respond(user_input)
        if response == "my_custom_function":
            arg= 'response='+response + f"('{user_input}')"
            print(arg)
            exec(arg)
        print(response)
    