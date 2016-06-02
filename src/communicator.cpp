#include "communicator.h"


namespace multiverso
{
	namespace wordembedding
	{
		template <typename T>
		void filler(std::vector<T> &v){
			std::random_device rd;
			std::mt19937 gen(rd());
			std::uniform_real_distribution<real> dis(-0.5 / embedding_size, 0.5 / embedding_size);

			for (int i = 0; i<v.size(); i++)
			{
				v[i] = dis(gen);
			}
		}

		Communicator::Communicator(Option* option){
			option_ = option;
			process_id_ = multiverso::MV_Rank();
			memory_mamanger_ = new MemoryManager(option_->embeding_size);
			embedding_size = option->embeding_size;
		}

		Communicator::~Communicator(){
			ClearParameterTables();
			delete memory_mamanger_;
		}

		void Communicator::PrepareParameterTables(int row_size, int column_size){
			worker_input_table_ = new MatrixWorkerTable<real>(row_size, column_size);
			worker_output_table_ = new MatrixWorkerTable<real>(row_size, column_size);
			server_input_table_ = new MatrixServerTable<real>(row_size, column_size, &filler);
			server_output_table_ = new MatrixServerTable<real>(row_size, column_size);

			worker_wordcount_table_ = new KVWorkerTable<int, int64>();
			server_wordcount_table_ = new KVServerTable<int, int64>();

			if (option_->use_adagrad){
				worker_input_gradient_table_ = new MatrixWorkerTable<real>(row_size, column_size);
				worker_output_gradient_table_ = new MatrixWorkerTable<real>(row_size, column_size);
				server_input_gradient_table_ = new MatrixServerTable<real>(row_size, column_size);
				server_output_gradient_table_ = new MatrixServerTable<real>(row_size, column_size);
			}
		}

		void Communicator::ClearParameterTables(){
			delete worker_input_table_;
			delete worker_output_table_;
			delete server_input_table_;
			delete server_output_table_;
			delete worker_wordcount_table_;
			delete server_wordcount_table_;

			if (option_->use_adagrad){
				delete worker_input_gradient_table_;
				delete worker_output_gradient_table_;
				delete server_input_gradient_table_;
				delete server_output_gradient_table_;
			}
			//multiverso::Log::Info("Rank %d Clear Parameter Tables done.\n", process_id_);
		}

		inline void Communicator::AddRows(MatrixWorkerTable<real>* table_, std::vector<int> row_ids, std::vector<real *> ptrs, int size){
			AddOption add_option;
			table_->Add(row_ids, ptrs, size, &add_option);
		}

		void Communicator::GetWorkerTableRows(std::vector<int> row_nums, std::vector<real*> &blocks, int embeding_size){
			worker_input_table_->Get(row_nums, blocks, embeding_size);
		}

		inline void Communicator::GetRows(MatrixWorkerTable<real>* table_, std::vector<int> row_ids, std::vector<real *> ptrs, int size){
			table_->Get(row_ids, ptrs, size);
		}

		inline void Communicator::RequestParameterByTableId(DataBlock *data_block, int table_id, std::vector<int> &nodes, std::vector<real*> &blocks){
			switch (table_id){
			case kInputEmbeddingTableId:
				GetRows(worker_input_table_, nodes, blocks, option_->embeding_size);
				SetDataBlockEmbedding(data_block, blocks, nodes, kInputEmbeddingTableId);
				break;
			case kEmbeddingOutputTableId:
				GetRows(worker_output_table_, nodes, blocks, option_->embeding_size);
				SetDataBlockEmbedding(data_block, blocks, nodes, kEmbeddingOutputTableId);
				break;
			case kSumGradient2IETableId:
				GetRows(worker_input_gradient_table_, nodes, blocks, option_->embeding_size);
				SetDataBlockEmbedding(data_block, blocks, nodes, kSumGradient2IETableId);
				break;
			case kSumGradient2EOTableId:
				GetRows(worker_output_gradient_table_, nodes, blocks, option_->embeding_size);
				SetDataBlockEmbedding(data_block, blocks, nodes, kSumGradient2EOTableId);
				break;
			}
		}

		inline void Communicator::SetDataBlockEmbedding(DataBlock *data_block, std::vector<real*> &blocks, std::vector<int> &nodes, int table_id){
			switch (table_id){
			case kInputEmbeddingTableId:
				for (int i = 0; i < nodes.size(); ++i){
					data_block->SetWeightIE(nodes[i], blocks[i]);
				}
				break;
			case kEmbeddingOutputTableId:
				for (int i = 0; i < nodes.size(); ++i){
					data_block->SetWeightEO(nodes[i], blocks[i]);
				}
				break;
			case kSumGradient2IETableId:
				for (int i = 0; i < nodes.size(); ++i){
					data_block->SetSumGradient2IE(nodes[i], blocks[i]);
				}
				break;
			case kSumGradient2EOTableId:
				for (int i = 0; i < nodes.size(); ++i){
					data_block->SetSumGradient2EO(nodes[i], blocks[i]);
				}
				break;
			}
		}

		void Communicator::RequestParameter(DataBlock *data_block)
		{
			clock_t start = clock();

			std::vector<int> input_nodes(data_block->input_nodes.begin(), data_block->input_nodes.end());
			std::vector<int> output_nodes(data_block->output_nodes.begin(), data_block->output_nodes.end());
			std::vector<real*> input_blocks;
			std::vector<real*> output_blocks;

			//Request blocks to store parameters
			memory_mamanger_->RequestBlocks(data_block->input_nodes.size(), input_blocks);
			memory_mamanger_->RequestBlocks(data_block->output_nodes.size(), output_blocks);
			assert(input_blocks.size() == data_block->input_nodes.size());
			assert(output_blocks.size() == data_block->output_nodes.size());

			RequestParameterByTableId(data_block, kInputEmbeddingTableId, input_nodes, input_blocks);
			RequestParameterByTableId(data_block, kEmbeddingOutputTableId, output_nodes, output_blocks);

			if (option_->use_adagrad){
				std::vector<real*> input_gradient_blocks;
				std::vector<real*> output_gradient_blocks;

				memory_mamanger_->RequestBlocks(input_nodes.size(), input_gradient_blocks);
				memory_mamanger_->RequestBlocks(output_nodes.size(), output_gradient_blocks);

				RequestParameterByTableId(data_block, kSumGradient2IETableId, input_nodes, input_gradient_blocks);
				RequestParameterByTableId(data_block, kSumGradient2EOTableId, output_nodes, output_gradient_blocks);
			}

			multiverso::Log::Info("Rank %d Request Parameters time:%lfs\n", process_id_,
				(clock() - start) / (double)CLOCKS_PER_SEC);
		}

		inline void Communicator::GetDeltaLoop(DataBlock *data_block, std::vector<real*> &blocks, std::vector<int> &nodes, int table_id, std::vector<real*> &recycle_blocks){
			std::function<real*(int)> get_function;
			switch (table_id){
			case kInputEmbeddingTableId:
				get_function = std::bind(&DataBlock::GetWeightIE, data_block, std::placeholders::_1);
				break;
			case kEmbeddingOutputTableId:
				get_function = std::bind(&DataBlock::GetWeightEO, data_block, std::placeholders::_1);
				break;
			case kSumGradient2IETableId:
				get_function = std::bind(&DataBlock::GetSumGradient2IE, data_block, std::placeholders::_1);
				break;
			case kSumGradient2EOTableId:
				get_function = std::bind(&DataBlock::GetSumGradient2EO, data_block, std::placeholders::_1);
				break;
			}

			for (int i = 0; i < nodes.size(); ++i)
			{
				real* new_row = get_function((nodes[i]));
				real* old_row = blocks[i];
				assert(new_row != nullptr);

				for (int j = 0; j < option_->embeding_size; ++j)
				{
					old_row[j] = (new_row[j] - old_row[j]) / option_->thread_cnt;
				}
				recycle_blocks.push_back(new_row);
			}
		}

		void Communicator::AddParameterByTableId(DataBlock *data_block, int table_id, std::vector<int> &nodes, std::vector<real*> &blocks, std::vector<real*> &recycle_blocks){
			switch (table_id){
			case kInputEmbeddingTableId:
				GetRows(worker_input_table_, nodes, blocks, option_->embeding_size);
				GetDeltaLoop(data_block, blocks, nodes, table_id, recycle_blocks);
				AddRows(worker_input_table_, nodes, blocks, option_->embeding_size);
				break;
			case kEmbeddingOutputTableId:
				GetRows(worker_output_table_, nodes, blocks, option_->embeding_size);
				GetDeltaLoop(data_block, blocks, nodes, table_id, recycle_blocks);
				AddRows(worker_output_table_, nodes, blocks, option_->embeding_size);
				break;
			case kSumGradient2IETableId:
				GetRows(worker_input_gradient_table_, nodes, blocks, option_->embeding_size);
				GetDeltaLoop(data_block, blocks, nodes, table_id, recycle_blocks);
				AddRows(worker_input_gradient_table_, nodes, blocks, option_->embeding_size);
				break;
			case kSumGradient2EOTableId:
				GetRows(worker_output_gradient_table_, nodes, blocks, option_->embeding_size);
				GetDeltaLoop(data_block, blocks, nodes, table_id, recycle_blocks);
				AddRows(worker_output_gradient_table_, nodes, blocks, option_->embeding_size);
				break;
			}
		}

		//Add delta to local buffer and send it to the parameter sever
		void Communicator::AddDeltaParameter(DataBlock *data_block)
		{
			if (data_block == nullptr){
				multiverso::Log::Info("Rank %d has null DataBlcok.\n", process_id_);
				return;
			}

			clock_t start = clock();
			std::vector<real*> blocks;
			std::vector<real*> recycle_blocks;

			std::vector<int> input_nodes(data_block->input_nodes.begin(), data_block->input_nodes.end());
			std::vector<int> output_nodes(data_block->output_nodes.begin(), data_block->output_nodes.end());
			std::vector<real*> input_blocks;
			std::vector<real*> output_blocks;
			//Request blocks to store parameters
			memory_mamanger_->RequestBlocks(input_nodes.size(), input_blocks);
			memory_mamanger_->RequestBlocks(output_nodes.size(), output_blocks);
			assert(input_blocks.size() == input_nodes.size());
			assert(output_blocks.size() == output_nodes.size());

			AddParameterByTableId(data_block, kInputEmbeddingTableId, input_nodes, input_blocks, recycle_blocks);
			AddParameterByTableId(data_block, kEmbeddingOutputTableId, output_nodes, output_blocks, recycle_blocks);

			memory_mamanger_->ReturnBlocks(input_blocks);
			memory_mamanger_->ReturnBlocks(output_blocks);

			if (option_->use_adagrad){
				std::vector<real*> input_gradient_blocks;
				std::vector<real*> output_gradient_blocks;
				memory_mamanger_->RequestBlocks(input_nodes.size(), input_gradient_blocks);
				memory_mamanger_->RequestBlocks(output_nodes.size(), output_gradient_blocks);

				AddParameterByTableId(data_block, kSumGradient2IETableId, input_nodes, input_gradient_blocks, recycle_blocks);
				AddParameterByTableId(data_block, kSumGradient2EOTableId, output_nodes, output_gradient_blocks, recycle_blocks);

				memory_mamanger_->ReturnBlocks(input_gradient_blocks);
				memory_mamanger_->ReturnBlocks(output_gradient_blocks);
			}

			memory_mamanger_->ReturnBlocks(recycle_blocks);
			multiverso::Log::Info("Rank %d Add Parameters time:%lfs\n", process_id_, (clock() - start) / (double)CLOCKS_PER_SEC);
		}

		int64 Communicator::GetWordCount(){
			std::unordered_map<int, int64> &kv_ = worker_wordcount_table_->raw();
			worker_wordcount_table_->Get(kWordCountId);
			return kv_[kWordCountId];
		}

		void Communicator::AddWordCount(int word_count_num){
			worker_wordcount_table_->Add(kWordCountId, word_count_num);
		}

		void Communicator::RequestBlocks(int size, std::vector<real*> &blocks){
			memory_mamanger_->RequestBlocks(size, blocks);
		}
		void Communicator::ReturnBlocks(std::vector<real*> &blocks){
			memory_mamanger_->ReturnBlocks(blocks);
		}
	}
}