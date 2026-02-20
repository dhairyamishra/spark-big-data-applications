# NYU Dataproc Cluster Commands - EXACT SEQUENCE

## Step 1: Connect to NYU Cluster
1. Go to: https://dataproc.hpc.nyu.edu/ssh  
2. Click blue "Connect" button
3. **SCREENSHOT #1**: Terminal showing connection banner + hostname + prompt

## Step 2A: HDFS Directory Operations  
```bash
hadoop fs -ls /                           # List HDFS root
hadoop fs -ls                             # List your home  
hadoop fs -ls /user/yourNetID_nyu_edu     # List your explicit path
```
**SCREENSHOT #2**: All three ls command outputs

## Step 2B: Create Directory
```bash
hadoop fs -mkdir lab1                     # Create folder
hadoop fs -ls                             # Verify creation  
hadoop fs -getfacl lab1                   # Check permissions
```
**SCREENSHOT #3**: Directory creation and permissions output

## Step 2C: File Upload
```bash
wget https://www.gutenberg.org/files/1661/1661-0.txt    # Download
hadoop fs -put 1661-0.txt lab1/data.txt                # Upload to HDFS
hadoop fs -ls lab1                                      # Verify upload
```
**SCREENSHOT #4**: Upload process and verification

## Step 2D: File Download & Verification  
```bash
hadoop fs -get lab1/data.txt              # Download from HDFS
diff 1661-0.txt data.txt                  # Compare (no output = success)
```
**SCREENSHOT #5**: diff command result (should be empty)

## Step 2E: Cleanup
```bash
hadoop fs -rm -r lab1                     # Delete directory
hadoop fs -ls                             # Verify deletion
```
**SCREENSHOT #6**: Cleanup confirmation

## Step 3: Spark Shell Commands
```bash
spark-shell --deploy-mode client
```

### Inside spark-shell (at scala> prompt):
```scala
:help
sc.                                       # Press Tab for autocomplete
sc.version  
val myConstant: Int = 2437
myConstant
myConstant.                               # Press Tab for autocomplete
myConstant.to                             # Press Tab for autocomplete  
myConstant.toFloat
myConstant
myConstant.toFloat.toInt
val myString = myConstant.toString
:type myString
:q                                        # Exit
```
**SCREENSHOT #7**: Spark startup + scala> prompt
**SCREENSHOT #8**: Running sc.version and variable commands
**SCREENSHOT #9**: :type myString result

## Final Screenshots Needed:
1. NYU cluster connection
2. HDFS ls commands
3. Directory creation + permissions
4. File upload + verification  
5. diff result (empty = success)
6. Cleanup confirmation
7. Spark shell startup
8. Scala commands execution
9. Variable type checking

## Important Notes:
- Replace `yourNetID_nyu_edu` with your actual NYU NetID
- Take screenshots as you go - don't wait until the end
- Ensure terminal output is clearly visible
- The diff command should produce NO OUTPUT if files match
