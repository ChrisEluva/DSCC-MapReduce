# DSCC-MapReduce
The goal of this assignment is to implement and test some modifications for the Map Reduce Application covered.

The specific version of map reduce implemented is one that converts a file from the following format:  
`"file1.txt" => "foo foo bar cat dog dog"`  
`"file2.txt" => "foo house cat cat dog"`  
`"file3.txt" => "foo foo foo bird"`  
to this  
`"foo" => { "file1.txt" => 2, "file3.txt" => 3, "file2.txt" => 1 }`    
`"bar" => { "file1.txt" => 1 }`  
`"cat" => { "file2.txt" => 2, "file1.txt" => 1 }`  
`"dog" => { "file2.txt" => 1, "file1.txt" => 2 }`  
`"house" => { "file2.txt" => 1 }`  
`"bird" => { "file3.txt" => 1 }  `

Link to assignment brief: https://drive.google.com/file/d/0B8VUpNlpyrPaTVlfZExqY1Z0a0k/view?usp=sharing

To run the assignment use the following steps:

- Navigate to the assignments src folder
- Run the following three commands (which will compile each java file)  

`javac threaded_version/MapReduce.java`  
`javac threadpool_version/MapReduce.java`  
`javac threadsafe_version/MapReduce.java`  

- Then, to run each version of the program use the following commands:  

`java threaded_version/MapReduce file1 file2 file3 file4 file5 file6 file7 file8 file9 file10 file11`  

`java threadpool_version/MapReduce [no of desired threads] file1 file2 file3 file4 file5 file6 file7 file8 file9 file10 file11`  

`java threadsafe_version/MapReduce [no of desired threads] file1 file2 file3 file4 file5 file6 file7 file8 file9 file10 file11`  

Each file is taken from the src/Text Files folder.  

The outputs of each program can be seen in the src/Output folder.
