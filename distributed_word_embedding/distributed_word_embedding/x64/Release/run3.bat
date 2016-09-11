distributed_word_embedding.exe -rank_num 2 -server_num 3 -is_pipeline 1 -alpha 0.01 -data_block_size 10000000 -use_adagrad 0 -train_file D:\data\aether\train_file_3.tsv -output D:\Workspace\git\distributed_word_embedding\example\output_3 -threads 5 -size 100 -binary 1 -cbow 1 -epoch 1 -negative 5 -hs 0 -sample 0 -min_count 5 -window 5 -read_vocab D:\data\aether\vocab_file.tsv -broker_address 10.172.142.50:12400#10.172.142.50:12401 ExperimentID=af355a25-f5c5-42c1-93bc-4f4d79db645d Owner=FAREAST\guoke Priority=3 NodeID=441cc1e8

pause
