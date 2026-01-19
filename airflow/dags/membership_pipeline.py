from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from include.file_ingestion import create_run, check_for_files
from include.error_handling import handle_invalid_files, no_files_found
from include.validation import verify_file_structure, consolidate_data,validate_application
from include.data_transformation import split_name, create_membership_id, create_above_18_field, format_date_of_birth

# Define the DAG
with DAG(
    dag_id='membership_pipeline',
    description='Process hourly membership applications.',
    schedule='@hourly'  # hourly schedule
) as dag:

    with TaskGroup(group_id='file_ingestion') as file_ingestion_group:
        # Task checks for incoming files and decides next step
        check_for_files_task = check_for_files(
            task_id_for_create_run="file_ingestion.create_run",
            task_id_for_no_files_found="file_ingestion.no_files_found"
        )
        # Creates a run if files are present
        create_run_task = create_run()
        # Stops pipeline gracefully if no files are found
        no_files_found_task = no_files_found(error_task_id="file_ingestion.check_for_files")

        check_for_files_task >> [create_run_task, no_files_found_task]

    with TaskGroup(group_id='file_validation') as file_validation_group:
        # Verifies structure of ingested files
        verify_file_structure_task = verify_file_structure(
            task_id_for_invalid_files="file_validation.handle_invalid_file_structure",
            task_id_for_valid_files="file_validation.consolidate_data"
            )
        
        # Handles invalid files if structure is incorrect
        handle_invalid_file_structure_task = handle_invalid_files.override(task_id="handle_invalid_file_structure")(
            error_task_id = "file_validation.verify_file_structure"
        )

        # Consolidates structured files into a uniform dataset
        structure_data_task = consolidate_data()

        # Branch: valid structure -> consolidate, invalid -> handle error
        verify_file_structure_task >> [handle_invalid_file_structure_task, structure_data_task]
        
        # Validate the contents of the application data
        validate_application_task = validate_application(
            struct_info=structure_data_task ,
            task_id_for_valid_application="data_transformation.split_name",
            task_id_for_invalid_application="file_validation.no_valid_application_stop"
            )
        
        structure_data_task >> validate_application_task
        
        # Stops gracefully if no valid application data exists
        no_valid_data_stop_task = EmptyOperator(task_id="no_valid_application_stop")

        validate_application_task >> [no_valid_data_stop_task]

        
    with TaskGroup(group_id='data_transformation') as data_transformation_group:
        split_name_task = split_name(
            task_id_for_validation_function="file_validation.validate_application"
        )
        create_above_18_field_task =create_above_18_field(split_name_task)
        format_date_of_birth_task = format_date_of_birth(create_above_18_field_task)
        create_membership_id_task = create_membership_id(format_date_of_birth_task)

    # Set the dependencies between the tasks
    
    # Once run is created, move into validation phase
    create_run_task >> file_validation_group
    # Valid applications continue to transformation phase
    validate_application_task >> data_transformation_group