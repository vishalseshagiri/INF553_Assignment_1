### Average Count and Spark Dataframes

#### Submission Directory Structure

- Vishal_Seshagiri_Description.pdf (instructions on how to execute the files)
- OutputFiles (4 output files for task 1 / task 2 / big / small)
- Solution (Python and jar files for execution)
Execution steps
- For Python files
	- Task 1
		- Small
		-
			```
			spark-submit
			Vishal_Seshagiri_task1.py \
			ml-latest-small/ratings.csv \
			python_small_task1
			```
		- Big
		-
			```
			spark-submit
			Vishal_Seshagiri_task1.py \
			ml-20m/ratings.csv \
			python_big_task1
			```
	- Task 2
		- Small
		-
			```
			spark-submit
			Vishal_Seshagiri_task2.py \
			ml-latest-small/ratings.csv \
			ml-latest-small/tags.csv \
			python_small_task2
			```
		- Big
		-
			```
			spark-submit
			Vishal_Seshagiri_task2.py \
			ml-20m/ratings.csv \
			ml-20m/tags.csv \
			python_big_task2
			```
- For Scala files
	- Task 1
		- Small
		-
			```
			spark-submit --class Task1 --master
			Vishal_Seshagiri_hw1.jar \
			ml-latest-small/ratings.csv \
			scala_small_task1/
			```
		- Big
		-
			```
			spark-submit --class Task2 --master
			Vishal_Seshagiri_hw1.jar \
			ml-20m/ratings.csv \
			scala_big_task1
			```
	- Task 2
		- Small
		-
			```
			spark-submit --class Task2 --master
			Vishal_Seshagiri_hw1.jar \
			ml-latest-small/ratings.csv \
			ml-latest-small/tags.csv \
			scala_small_task2
			```
		- Big
		-
			```
			spark-submit --class Task2 --master
			Vishal_Seshagiri_hw1.jar \
			ml-20m/ratings.csv \
			ml-20m/tags.csv \
			scala_big_task2
			```