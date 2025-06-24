from loguru import logger
from typing import Any
from great_expectations.core.expectation_validation_result import ExpectationValidationResult
from great_expectations.core.batch_definition import BatchDefinition

def check_solution(task: int, result: Any) -> None:
    assert isinstance(task, int), "Task must be an integer."

    match task:
        case 1:
            assert isinstance(result, ExpectationValidationResult), "Result must be an instance of ExpectationValidationResult."
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
            assert isinstance(result, ExpectationValidationResult), "Result must be an instance of ExpectationValidationResult."
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
            assert isinstance(result, ExpectationValidationResult), "Result must be an instance of ExpectationValidationResult."
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
            assert isinstance(result, ExpectationValidationResult), "Result must be an instance of ExpectationValidationResult."
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
        case 5:
            assert isinstance(result, ExpectationValidationResult), "Result must be an instance of ExpectationValidationResult."
            if result["success"] != False:
                logger.error("The expectation should fail, check again.")
                return
            if result["expectation_config"]["type"] != "expect_column_values_to_match_regex":
                logger.error("The expectation type is not correct, check again.")
                return
            if result["expectation_config"]["kwargs"]["column"] != "dteday":
                logger.error("The column name is not correct, check again.")
                return        
            if not ((result["result"]["unexpected_count"] == 3) or (result["result"]["unexpected_count"] == 4)):
                logger.error("The unexpected count is not correct, check again.")
                return
            if result["result"]["unexpected_count"] == 3:
                logger.warning("There is another unexpected value to be found with a more advanced regex pattern.")
        case 6:
            if not result["success"]:
                logger.error("The validation suite should be successful, check again.")
                return
            if len(result["results"]) != 2:
                logger.error(f"Expected 2 expectations, but found {len(result['results'])}.")
                return
            
            # Check first expectation
            exp1 = result["results"][0]
            if not exp1["success"]:
                logger.error("The first expectation should pass, check again.")
                return
            if exp1["expectation_config"]["type"] != "expect_column_values_to_not_be_null":
                logger.error(f"The first expectation type is not correct, got {exp1['expectation_config']['type']}, expected expect_column_values_to_not_be_null.")
                return
            if exp1["expectation_config"]["kwargs"]["column"] != "total":
                logger.error(f"The first column name is not correct, got {exp1['expectation_config']['kwargs']['column']}, expected total.")
                return
            
            # Check second expectation
            exp2 = result["results"][1]
            if not exp2["success"]:
                logger.error("The second expectation should pass, check again.")
                return
            if exp2["expectation_config"]["type"] != "expect_column_values_to_not_be_null":
                logger.error(f"The second expectation type is not correct, got {exp2['expectation_config']['type']}, expected expect_column_values_to_not_be_null.")
                return
            if exp2["expectation_config"]["kwargs"]["column"] != "dteday":
                logger.error(f"The second column name is not correct, got {exp2['expectation_config']['kwargs']['column']}, expected dteday.")
                return
            
            # Check overall statistics
            if result["statistics"]["evaluated_expectations"] != 2:
                logger.error(f"Expected 2 evaluated expectations, got {result['statistics']['evaluated_expectations']}.")
                return
            if result["statistics"]["successful_expectations"] != 2:
                logger.error(f"Expected 2 successful expectations, got {result['statistics']['successful_expectations']}.")
                return
        case 7:
            if not result["success"]:
                logger.error("The validation suite should be successful, check again.")
                return
            if len(result["results"]) != 3:
                logger.error(f"Expected 3 expectations, but found {len(result['results'])}.")
                return
            
            # Check first expectation - total not null
            exp1 = result["results"][0]
            if not exp1["success"]:
                logger.error("The first expectation should pass, check again.")
                return
            if exp1["expectation_config"]["type"] != "expect_column_values_to_not_be_null":
                logger.error(f"The first expectation type is not correct, got {exp1['expectation_config']['type']}, expected expect_column_values_to_not_be_null.")
                return
            if exp1["expectation_config"]["kwargs"]["column"] != "total":
                logger.error(f"The first column name is not correct, got {exp1['expectation_config']['kwargs']['column']}, expected total.")
                return
            if exp1["result"]["unexpected_count"] != 0:
                logger.error(f"Expected no null values in 'total' column, but found {exp1['result']['unexpected_count']}.")
                return
            
            # Check second expectation - dteday not null
            exp2 = result["results"][1]
            if not exp2["success"]:
                logger.error("The second expectation should pass, check again.")
                return
            if exp2["expectation_config"]["type"] != "expect_column_values_to_not_be_null":
                logger.error(f"The second expectation type is not correct, got {exp2['expectation_config']['type']}, expected expect_column_values_to_not_be_null.")
                return
            if exp2["expectation_config"]["kwargs"]["column"] != "dteday":
                logger.error(f"The second column name is not correct, got {exp2['expectation_config']['kwargs']['column']}, expected dteday.")
                return
            if exp2["result"]["unexpected_count"] != 0:
                logger.error(f"Expected no null values in 'dteday' column, but found {exp2['result']['unexpected_count']}.")
                return
            
            # Check third expectation - season values
            exp3 = result["results"][2]
            if not exp3["success"]:
                logger.error("The third expectation should pass, check again.")
                return
            if exp3["expectation_config"]["type"] != "expect_column_values_to_be_in_set":
                logger.error(f"The third expectation type is not correct, got {exp3['expectation_config']['type']}, expected expect_column_values_to_be_in_set.")
                return
            if exp3["expectation_config"]["kwargs"]["column"] != "season":
                logger.error(f"The third column name is not correct, got {exp3['expectation_config']['kwargs']['column']}, expected season.")
                return
            expected_value_set = ["Spring", "Summer"]
            if set(exp3["expectation_config"]["kwargs"]["value_set"]) != set(expected_value_set):
                logger.error(f"The value set is not correct, got {exp3['expectation_config']['kwargs']['value_set']}, expected {expected_value_set}.")
                return
            if exp3["result"]["unexpected_count"] != 0:
                logger.error(f"Expected no unexpected values in 'season' column, but found {exp3['result']['unexpected_count']}.")
                return
            
            # Check overall statistics
            if result["statistics"]["evaluated_expectations"] != 3:
                logger.error(f"Expected 3 evaluated expectations, got {result['statistics']['evaluated_expectations']}.")
                return
            if result["statistics"]["successful_expectations"] != 3:
                logger.error(f"Expected 3 successful expectations, got {result['statistics']['successful_expectations']}.")
                return
            if result["statistics"]["success_percent"] != 100.0:
                logger.error(f"Expected 100% success rate, got {result['statistics']['success_percent']}%.")
                return
        case 8:
            assert isinstance(result, BatchDefinition), "Result must be an instance of BatchDefinition."
            result.partitioner.method_name = "partition_on_year_and_month"
            result.partitioner.column_name = "dteday"
        case 9:
            if not result["success"]:
                logger.error("The expectation should pass, check again.")
                return
            if result["expectation_config"]["type"] != "unexpected_rows_expectation":
                logger.error(f"The expectation type is not correct, got {result['expectation_config']['type']}, expected unexpected_rows_expectation.")
                return
            if result["expectation_config"]["kwargs"].get("unexpected_rows_query") is None:
                logger.error("The unexpected_rows_query parameter is missing.")
                return
            if result["result"]["observed_value"] != 0:
                logger.error(f"Expected observed value to be 0, got {result['result']['observed_value']}.")
                return
            if not isinstance(result["result"]["details"]["unexpected_rows"], list):
                logger.error("The unexpected_rows should be a list.")
                return
            if len(result["result"]["details"]["unexpected_rows"]) != 0:
                logger.error(f"Expected no unexpected rows, but found {len(result['result']['details']['unexpected_rows'])}.")
                return
        case _:
            logger.error(f"Unknown task: {task}. Please provide a valid task number.")
            return

    logger.success("Great job! The result of the expectation is correct. Continue with the next task.")