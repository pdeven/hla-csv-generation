# hla-csv-generation

**Usage**
```python
python3 hla_service_automation_wrapper.py -h
```
- Place xlxs and csv file inside the input directory and wait for the mail

```mermaid
graph TD;
    wrapper-->main;
    main-->serology;
    main-->mail generation;
    main-->csv;
```
