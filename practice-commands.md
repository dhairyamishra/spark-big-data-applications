# Local Practice Commands Guide

## Getting Started
1. Run `setup-local-hadoop.bat` to start the environment
2. Run `connect-hadoop.bat` to enter the container
3. Practice all commands below before using NYU cluster

## Step 2A: HDFS Directory Exploration
```bash
# List HDFS root directory
hadoop fs -ls /

# List your home directory  
hadoop fs -ls

# List your home directory explicitly (replace 'root' with actual user)
hadoop fs -ls /user/root
```

## Step 2B: Create Directory and Check Permissions
```bash
# Create lab1 folder
hadoop fs -mkdir lab1

# Verify creation
hadoop fs -ls

# Check permissions
hadoop fs -getfacl lab1
```

## Step 2C: Upload File to HDFS
```bash
# Download the book file
wget https://www.gutenberg.org/files/1661/1661-0.txt

# Upload to HDFS with new name
hadoop fs -put 1661-0.txt lab1/data.txt

# Verify upload
hadoop fs -ls lab1
```

## Step 2D: Download and Verify
```bash
# Download from HDFS
hadoop fs -get lab1/data.txt

# Compare files (should show no output if identical)
diff 1661-0.txt data.txt
```

## Step 2E: Cleanup
```bash
# Delete the lab1 directory
hadoop fs -rm -r lab1

# Verify deletion
hadoop fs -ls
```

## Step 3: Spark Shell Commands
```bash
# Start Spark shell
spark-shell --deploy-mode client
```

### Inside Spark shell (at scala> prompt):
```scala
:help
sc. (then press Tab)
sc.version
val myConstant: Int = 2437
myConstant
myConstant. (then press Tab)
myConstant.to (then press Tab)
myConstant.toFloat
myConstant
myConstant.toFloat.toInt
val myString = myConstant.toString
:type myString
:q
```

## Screenshot Checklist for Practice
- [ ] Container connection successful
- [ ] HDFS directory listings  
- [ ] Directory creation and permissions
- [ ] File upload and verification
- [ ] diff command result (empty = success)
- [ ] Directory cleanup
- [ ] Spark shell startup
- [ ] Scala commands execution

## Notes
- Practice here first to get familiar with commands
- Then repeat on NYU cluster for final screenshots
- Container user will be 'root' instead of your NYU NetID
- Commands are identical to what you'll use on NYU cluster
