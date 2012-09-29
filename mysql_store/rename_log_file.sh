D='./data'
for f in `ls $D`;do 
    nf=${f%.1};
    if [ "$f" != "$nf" ]; then
        mv $D/$f $D/$nf;
    fi
done
