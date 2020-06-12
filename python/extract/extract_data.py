from prefect import Parameter, Flow

import get_datamelbourne as get_datamelbourne

f = Flow("ELT")
f.add_task(get_datamelbourne.get_music_venue)

state = f.run()

assert state.is_successful()
