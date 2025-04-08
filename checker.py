from loguru import logger
from great_expectations.core.expectation_validation_result import ExpectationValidationResult

def check(task: int, result: ExpectationValidationResult) -> None:
    assert isinstance(task, int)
    assert isinstance(result, ExpectationValidationResult)

    match task:
        case 1:
            if not result["success"]:
                logger.error("The expectation was not met, check the result.")
                return
            if result["result"]["unexpected_count"] != 0:
                logger.error("The unexpected count is not zero.")
                return

        case _:
            logger.error(f"Unknown task: {task}. Please provide a valid task number.")
            return

    logger.success("Great job! The result is as expected.")


