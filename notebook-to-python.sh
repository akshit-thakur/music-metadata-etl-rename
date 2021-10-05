if [ $# -eq 0 ]; then
    echo "No arguments provided"
    exit 1
fi

jupyter nbconvert $1 --to python
