from loguru import logger
from great_expectations.core.expectation_validation_result import ExpectationValidationResult

def check(task: int, result: ExpectationValidationResult) -> None:
    assert isinstance(task, int), "Task must be an integer."
    assert isinstance(result, ExpectationValidationResult), "Result must be an instance of ExpectationValidationResult."

    match task:
        case 1:
            if not result["success"]:
                logger.error("The expectation was not met, check again.")
                return
            if result["expectation_config"]["type"] != "expect_column_to_exist":
                logger.error("The expectation type is not correct, check again.")
                return
            if result["expectation_config"]["kwargs"]["column"] != "season":
                logger.error("The column name is not correct, check again.")
                return

        case _:
            logger.error(f"Unknown task: {task}. Please provide a valid task number.")
            return

    logger.success("Great job! The result of the expectation is correct. Continue with the next task.")

