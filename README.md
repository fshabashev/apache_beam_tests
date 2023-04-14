## Steps to run the project
#### 1. [Optionally] Create and activate a virtual environment
```python3 -m venv venv```

```source venv/bin/activate```
#### 2. [Optionally] Install apache-beam and pandas
```python -m pip install -r requirements.txt```
#### 3. Run the pandas script:
```python pandas_solution.py```
#### 4. Run the apache beam script:
```python apache_beam_solution.py```

#### 5. [Optionally] Compare the outputs of the two scripts:
```python3 compare_outputs.py```

## Notes
The apache beam script is configured to run on a unix machine, so all line endings are converted to unix line endings.