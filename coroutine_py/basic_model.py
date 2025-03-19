import time

# First topic is still about coroutine
# TODO: compare this to the python official version to get the basic concepts and working mechanisms of coroutine model
# TODO: advanced a bit is to support the coroutine with multi-threading implementation using python way?

# Second topic is using the coroutine model with system domains like durable funcitons like Temporal

_PENDING = "pending"
_DONE = "done"
# TODO: feature-2
_CANCELLED = "cancelled"
# TODO: feature-add timer, suspended not just ready?

# can be threadlocal, depending on the design pattern
_current_loop = None

# Future shall be a simple 
# result, state, and callbacks with itself as the parameter
# set result with trigger callbacks that's why Future connects things
class _Future:
    def __init__(self, loop=None, callback: callable=None):
        self.callback = callback # only support one callback for now
        self.result = None
        self.state = "pending"
        self.loop = loop
        if loop == None:
            self.loop = _Eventloop.get_current_eventloop()

    def set_callback(self, callback: callable):
        self.callback = callback

    def set_result(self, res=None):
        self.result = res
        self.state = "done"
        if self.callback != None:
            self.loop.call_now(None, self.callback, self)

class _Eventloop:
    def __init__(self):
        self.ready = []
        self._future_stack = []

    @staticmethod
    def get_current_eventloop():
        global _current_loop
        if _current_loop == None:
            _current_loop = _Eventloop()
        return _current_loop

    def call_now(self, future: _Future, func: callable, *args, **kwargs): # type: ignore
        # if future == None:
        #     future = _Future()
        self.ready.append((future, func, args, kwargs))

    def run(self):
        while self.ready:
            future, func, args, kwargs = self.ready.pop()
            if future is not None:
                self._future_stack.append(future)
            try:
                next_future = func(*args, **kwargs)
                # TODO: here has a core problem
                # func is not finished, we need to wait func to 
                # finish and then call this future's callback again
                # or our tasks are lost
            except _CoroutineStop as es:
                if self._future_stack:
                    future = self._future_stack.pop()
                    if future:
                        future.set_result(es.get_value())
                # future.set_result(es.get_value())

class _CoroutineStop(Exception):
    def __init__(self, value:any):
        self.value = value
    def get_value(self):
        return self.value

# CoroutineContext is a simple state machine
class _CoroutineContext:
    def __init__(
            self, params:dict={}, *, 
            state:int=0, history: dict={}, current_coro=""):
        self.state = state
        self.history = history
        self.current_coro = None
        self.params = params
        self.coroutine = None

    def set_coroutine(self, func: callable):
        self.coroutine = func

    def resume(self, future: _Future):
        print(f"RESUME -- to {self.coroutine.__name__} state {self.state}")
        self.history[self.current_coro] = future.result
        if self.coroutine != None:
            self.coroutine(self)

def coroutine_simple_1(message:str):
    # TODO: right now we don't await on multiple futures
    def _coroutine_simple_1(con:_CoroutineContext=_CoroutineContext()):
        loop = _Eventloop.get_current_eventloop()
        if con.state == 0:
            print(f"simple 1 executed with message {con.params.get('message')}")
            con.state = 1
            ftr1 = _Future()
            ftr1.set_callback(con.resume)
            loop.call_now(ftr1, coroutine_simple_2, "call from simple 1--first time")
            print(f"YIELD -- from _coroutine_simple_1 state {con.state}")
            return ftr1
        if con.state == 1:
            print("_coroutine_simple_1 completes")
            raise _CoroutineStop("result_activity_1")

    con = _CoroutineContext()
    con.params["message"] = message
    con.set_coroutine(_coroutine_simple_1)
    return _coroutine_simple_1(con)

def coroutine_simple_2(message:str):
    def _coroutine_simple_2(con:_CoroutineContext=_CoroutineContext()):
        print(f"simple 2 executed with message {con.params.get('message')}")
        raise _CoroutineStop("result_activity_2")

    con = _CoroutineContext()
    con.params["message"] = message
    con.set_coroutine(_coroutine_simple_2)
    return _coroutine_simple_2(con)

def common_workflow(name: str):
    def _common_workflow(con:_CoroutineContext=_CoroutineContext()):
        # print(f"common workflow {con.params} called with state= {con.state}")
        loop = _Eventloop.get_current_eventloop()
        name = con.params.get("name")
        # state before the chained coroutine call
        if con.state == 0:
            print(f"Step {con.state}: local calculation of {name}")
            # for real coroutine, this should be the stack
            calculation_result = 1000
            con.history["calculation_result"] = calculation_result
            con.state = 1

        if con.state == 1:
            print(f"Step {con.state}: calls simple activity 1 of {name}")
            # save the state of caller coroutine
            con.state = 2
            con.current_coro = "coroutine_simple_1"

            future = _Future()
            # maki: very important to add resume to future when await on some future
            future.set_callback(con.resume)
            # maki: key to register the coroutine and future together
            loop.call_now(future, coroutine_simple_1, "first message")
            print(f"YIELD --- _common_workflow from state {con.state}")
            return future

        # state after the coroutine call
        if con.state == 2:
            print(f"Step {con.state} calls simple activity 2 of {name}")
            con.state = 3
            con.current_coro = "coroutine_simple_2"
            message = con.history.get("coroutine_simple_1")

            ftr2 = _Future(callback=con.resume)
            ftr2.set_callback(con.resume)
            loop.call_now(ftr2, coroutine_simple_2, message)
            print(f"YIELD --- _common_workflow from state {con.state}")
            return ftr2

        calculation_result = con.history.get("calculation_result")
        final_result = calculation_result + 100
        print(f"Step {con.state}: final result is {final_result}")
        raise _CoroutineStop(final_result)

    con = _CoroutineContext(params={"name": name})
    con.set_coroutine(_common_workflow)
    return _common_workflow(con)

if __name__ == "__main__":
    c_runtime = _Eventloop.get_current_eventloop()
    c_runtime.call_now(None, common_workflow, "GLOVER")
    # c_runtime.call_now(None, common_workflow, "REBETA")
    c_runtime.run()
