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
        
        case 2:
            if result["success"] != False:
                logger.error("The expectation should fail, check again.")
                return
            if result["expectation_config"]["type"] != "expect_column_values_to_be_in_set":
                logger.error("The expectation type is not correct, check again.")
                return
            if result["expectation_config"]["kwargs"]["column"] != "season":
                logger.error("The column name is not correct, check again.")
                return
            if result["result"]["unexpected_list"] != ["Sprung"]:
                logger.error("The list of unexpected values is not correct, check again.")
                return
            
        case 3:
            if result["success"] != True:
                logger.error("The expectation should pass now, check again.")
                return
            if result["expectation_config"]["type"] != "expect_column_values_to_be_in_set":
                logger.error("The expectation type is not correct, check again.")
                return
            if result["expectation_config"]["kwargs"]["column"] != "season":
                logger.error("The column name is not correct, check again.")
                return
            if result["result"]["unexpected_list"] != []:
                logger.error(f"There are still unexpected values, check again. Unexpected values: {result['result']['unexpected_list']}")
                return
        case 4:
            if result["success"] != True:
                logger.error("The expectation should pass now, check again.")
                return
            if result["expectation_config"]["type"] != "expect_column_max_to_be_between":
                logger.error("The expectation type is not correct, check again.")
                return
            if result["expectation_config"]["kwargs"]["column"] != "total":
                logger.error("The column name is not correct, check again.")
                return
            if result["result"]["observed_value"] != 638:
                logger.error(f"The observed value is not correct, check again. Observed value: {result['result']['observed_value']}")
                return
        case _:
            logger.error(f"Unknown task: {task}. Please provide a valid task number.")
            return

    logger.success("Great job! The result of the expectation is correct. Continue with the next task.")