"""
    gpt.py

    This handles the complexity of working with GPT.  Specifically it:

    1.  Takes in a user request and returns the final response from GPT
    2.  Handles function calls and then resubmitting the results to GPT to get an answer
    3.  Maintaining the chat history

"""
import copy
import json
import os
from datetime import datetime
import openai
from plog import Plog


class OpenAI():
    def __init__(self, system_prompt, funcs, timeout=30, history_max=12):
        self.funcs = funcs
        self.timeout = timeout
        openai.api_key = os.getenv('OPENAI_API_KEY')
        self.history = []
        self.system_prompt = system_prompt
        self.history_max = history_max

    def execute_function_call(self, message):
        if message["function_call"]["name"] == "ask_database":
            query = json.loads(message["function_call"]["arguments"])["query"]
            results = self.funcs.ask_database(query)
        else:
            results = f"Error: function {message['function_call']['name']} does not exist"

        return results

    # This is the actual interface for calling GPT
    def call_gpt(self, messages):
        # p_model = "gpt-4"             # WARNING: THIS GETS EXPENSIVE REAL FAST / BREAK GLASS ONLY
        p_model = "gpt-3.5-turbo"
        p_temperature = 0.01

        # We don't want to change the original set
        new_mess = copy.deepcopy(messages)

        #   Sneak a time into the system prompt
        for i in range(len(new_mess)):
            if new_mess[i]['role'] == 'system':
                new_mess[i]['content'] = new_mess[i][
                                             'content'] + '\nThe current time and date is ' + datetime.now().strftime(
                    "%Y-%m-%d %H:%M")

        Plog.info('Size of messages: ' + str(len(json.dumps(new_mess))))

        try:
            resp = openai.ChatCompletion.create(model=p_model, temperature=p_temperature, messages=new_mess,
                                                functions=self.funcs.get_functions_parameter(),
                                                request_timeout=self.timeout)

        except (openai.error.ServiceUnavailableError, openai.error.Timeout) as e:
            resp = {
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "I'm very sorry, but OpenAI is overloaded at the moment.  Please ask again",
                    },
                    "finish_reason": "stop"
                }]
            }
            Plog.error('GPT error: ' + str(e))


        return resp

    def call_gpt_handle_functions(self, inbound_message):

        #   Step 1: call GPT ...
        response = self.call_gpt(inbound_message)
        Plog.debug('GPT Responds: ' + str(response))

        #   Step 2: is this a regular response (ie, not a function_call)
        message = response["choices"][0]["message"]

        if message.get("function_call") is None:
            return response

        # Step 3: Handle a function call
        Plog.info(f'GPT asks to call {message["function_call"]["name"]}')

        #   Generate a summary of the function call data (to keep history as small as possible)
        short_resp = {
            "role": "assistant",
            "content": None,
            "function_call": {
                "name": message["function_call"]["name"],
                "arguments": message["function_call"]["arguments"]
            }
        }

        # Save it in our history as well as current working structure for later
        self.history.append(short_resp)
        inbound_message.append(short_resp)

        # Perform the function call

        results = self.execute_function_call(message)

        # Generate the entry in the history for the response
        func_results = {"role": "function", "name": message["function_call"]["name"], "content": results}

        # Add the response of the function call to the history
        self.history.append(func_results)

        # Add it to our next call too
        inbound_message.append(func_results)

        Plog.info('Calling GPT with results: ' + results.replace('\n', ' <p> ')[:40])

        # And now (tail-) recurse to either get a response or yet another function call (it happens!)
        return self.call_gpt_handle_functions(inbound_message)

    # This is the main entry point where we have some text we want to feed to a gpt completion
    def ask_gpt(self, user):

        Plog.info(f'Ask GPT: {user}')
        # Append the new message to our history; this will add it to the outbound request too
        self.history.append({"role": "user", "content": user})

        # Build up the messages structure for GPT
        messages = [{"role": "system", "content": self.system_prompt}]

        # Add in what's gone on before (and our latest user question)
        for h in self.history:
            messages.append(h)

        # When this returns, we're guaranteed a non-function call response
        # as all intermediate function work has been handled.
        response = self.call_gpt_handle_functions(messages)

        answer = response["choices"][0]["message"]["content"]

        Plog.info(f'GPT Response: {answer}')

        self.history.append({"role": "assistant", "content": answer})

        # Trim back our history
        self.history = self.history[-self.history_max:]

        return answer
