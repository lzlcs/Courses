test_name=$1

output_file_name=tests/userprog/$1.output
result_file_name=tests/userprog/$1.result

cd build
if [ -f "$output_file_name" ]; then

    rm $output_file_name

fi

make $result_file_name
cd ..