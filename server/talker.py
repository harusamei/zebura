import json

import chainlit as cl
import asyncio
import sys
sys.path.insert(0, '../')
from zebura_core.query_parser.parser import Parser



table_name = 'product'
parser = Parser()

@cl.step
def tool(message):

    result = asyncio.run(parser.apply(table_name, message.content))
    return str(result)


# @cl.step
# def msg(message):
#     return message

@cl.on_message  # this function will be called every time a user inputs a message in the UI
async def main(message: cl.Message):
    """
    This function is called every time a user inputs a message in the UI.
    It sends back an intermediate response from the tool, followed by the final answer.

    Args:
        message: The user's message.

    Returns:
        None.
    """

    # msg( message )

    # Call the tool
    tool(message)

    # Send the final answer.
    await cl.Message(content="This is the final answer").send()



