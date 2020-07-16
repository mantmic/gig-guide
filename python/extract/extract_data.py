from prefect.engine.executors import DaskExecutor

import flows.thebrag        as thebrag
import flows.reverbnation   as reverbnation



def main():
    if(True):
        thebrag_state = thebrag.flow.run()
        assert thebrag_state.is_successful()

    if(True):
        reverbnation_state = reverbnation.flow.run()
        assert reverbnation_state.is_successful()

if __name__ == "__main__":
    main()
