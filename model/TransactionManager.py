from Transaction import Transaction
from TransactionStatus import TransactionStatus
from LockType import LockType
from LockAcquireStatus import LockAcquireStatus
from collections import defaultdict
from InstructionType import InstructionType
import logging

log = logging.getLogger(__name__)

class TransactionManager():
    def __init__(self, site_manager, locktable) -> None:

        self.transaction_list = dict()
        self.current_time = 0
        self.site_manager = site_manager
        self.locktable = locktable
        self.transaction_blocked = dict()
        self.transaction_wait = dict()

    def execute(self, instruction):
        self.current_time += 1
        self.free_abort()
        self.deadlock_detect_and_clear()
        self.block_to_wait()
        self.test_waiting()

        parameters = instruction.get_parameters()
        ins_type = instruction.get_instruction_type()

        if ins_type == InstructionType.Begin:
            self.begin(parameters)
        elif ins_type == InstructionType.BeginRO:
            self.begin_RO(parameters)
        elif ins_type == InstructionType.Read:
            self.read(parameters)
        elif ins_type == InstructionType.Write:
            self.write(parameters)
        elif ins_type == InstructionType.End:
            self.end(parameters)


    def begin(self, parameters):
        
        log.info("Creating Transaction" + parameters[0])

        transaction_id = len(self.transaction_list)
        transaction = Transaction(id= transaction_id, name=parameters[0], status=TransactionStatus.Running, is_read_only=False)
        self.transaction_list[parameters[0]] = transaction



    def begin_RO(self, parameters):
        
        log.info("Creating Read Only Transaction" + parameters[0])

        transaction_id = len(self.transaction_list)
        transaction = Transaction(id= transaction_id, name=parameters[0], status=TransactionStatus.Creating, is_read_only=True)
        transaction.read_only_values = self.site_manager.get_current_variables()

        self.transaction_list[parameters[0]] = transaction

    def flatten_transaction_block(self):

        flatten_transaction_block = dict()

        for block_transactions in self.transaction_blocked.values():

            for transactions in list(block_transactions):

                info_tuple = block_transactions[transactions]

                if transactions not in flatten_transaction_block:

                    flatten_transaction_block[transactions] = [info_tuple]

                else:
                    flatten_transaction_block[transactions].append(info_tuple)
        
        return flatten_transaction_block

    def flatten_transaction_waiting(self):

        flatten_transaction_wait = dict()

        for waiting_transactions in self.transaction_wait.values():

            for transactions in list(waiting_transactions):

                info_tuple = waiting_transactions[transactions]

                if transactions not in flatten_transaction_wait:
                    flatten_transaction_wait[transactions] = [info_tuple]

                else:
                    flatten_transaction_wait[transactions].append(info_tuple)

        return flatten_transaction_wait


    def write(self, parameters):
        
        transaction_name = parameters[0]
        variable_name = parameters[1]
        value = int(parameters[2])

        if transaction_name not in self.transaction_list:
            log.info("Transaction " + transaction_name + " not exist")
            return

        target_transaction = self.transaction_list[transaction_name]

        if(target_transaction.status != TransactionStatus.Running and target_transaction.status != TransactionStatus.Waiting):
            return

        if self.locktable.is_locked_by_transaction(target_transaction, variable_name, LockType.Write):
            # check if the transaction has a key on the variable
            log.info(transaction_name + " already has a write lock on variable " + variable_name)
            self.locktable.add_lock(LockType.Write, target_transaction, variable_name)

            target_transaction.variable_commit[variable_name] = value
            target_transaction.set_status(TransactionStatus.Running)
            return

        is_acquire = self.site_manager.get_locks(target_transaction, LockType.Write, variable_name)

        if is_acquire == LockAcquireStatus.GotLock:

            self.locktable.add_lock(LockType.Write, target_transaction, variable_name)

            log.info(transaction_name + " acquire a write lock on " + variable_name)

            target_transaction.variable_commit[variable_name] = value

            flatten_transaction_block = flatten_transaction_block()

            if transaction_name in flatten_transaction_block:

                for info_tuple in flatten_transaction_block[transaction_name]:

                    if info_tuple[1] != variable_name:
                        return
            target_transaction.set_status(TransactionStatus.Running)

        elif is_acquire == LockAcquireStatus.AllSitesDown:

            info_tuple = (InstructionType.Write, variable_name, value)

            flatten_transaction_wait = self.flatten_transaction_waiting()

            if transaction_name in flatten_transaction_wait:

                if info_tuple in flatten_transaction_wait[transaction_name]:
                    return
            
            log.info("All sites down! "+ transaction_name + " is waiting on variable " + variable_name)

            target_transaction.set_status(TransactionStatus.Waiting)

            if self.current_time not in self.transaction_wait:
                self.transaction_wait[self.current_time] = {}
                self.transaction_wait[self.current_time][transaction_name] = info_tuple
            else:
                self.transaction_wait[self.current_time][transaction_name] = info_tuple

        
        else:
            for lock in self.site_manager.get_set_locks().locktable[variable_name]:

                if lock.transaction == target_transaction:
                    continue
                
                transaction_blocking = lock.transaction.name

                info_tuple = (transaction_blocking, InstructionType.Write, variable_name, value)

                flatten_transaction_block = self.flatten_transaction_block()

                if transaction_name in flatten_transaction_block:

                    if info_tuple in flatten_transaction_block[transaction_name]:
                        return

                    log.info(transaction_name + " is blocked by " + transaction_blocking + " for write request on " + variable_name)

                    self.current_time += 1

                    if self.current_time not in self.transaction_blocked:
                        self.transaction_blocked[self.current_time] = {}
                        self.transaction_blocked[self.current_time][transaction_name] = info_tuple
                    else:
                        self.transaction_blocked[self.current_time][transaction_name] = info_tuple


    def read(self, parameters, wait_tag= False):
        transaction_name = parameters[0]
        variable_name = parameters[1]

        if transaction_name not in self.transaction_list:
            log.info("Transaction " + transaction_name + " not exist")
            return

        target_transaction = self.transaction_list[transaction_name] 

        if(target_transaction.status != TransactionStatus.Running and target_transaction.status != TransactionStatus.Waiting):
            return

        if target_transaction.is_read_only == True:
            self.read_only(parameters, wait_tag)
        
        else:
            if self.locktable.is_locked_by_transaction(target_transaction, variable_name, LockType.Write):

                read_value = target_transaction.variable_commit[variable_name]

                log.info(transaction_name + "acquire a read lock on " + variable_name + " with variable value " + str(read_value))

                if variable_name not in target_transaction:
                    target_transaction.variable_read[variable_name] = []
                
                target_transaction.variable_read[variable_name].append(read_value)

                return

            if self.locktable.is_locked_by_transaction(target_transaction, variable_name, LockType.Read):
                self.locktable.add_lock(LockType.Read, target_transaction, variable_name)
                log.info(transaction_name + " already has a read key on "+ variable_name)
                target_transaction.set_status(TransactionStatus.Running)
                return

            for time_step in list(self.transaction_blocked):

                info_tuple_dict = self.transaction_blocked[time_step]

                for key in list(info_tuple_dict):

                    info_tuple = info_tuple_dict[key]

                    if len(info_tuple) == 4 and info_tuple[2] == variable_name:

                        for lock in self.locktable.locktable[variable_name]:

                            if lock.transaction == target_transaction:
                                continue

                            transaction_block = lock.transaction.name

                            block_info_tuple = (transaction_block, InstructionType.Read, variable_name)

                            target_transaction.set_status(TransactionStatus.Block)

                            self.transaction_blocked[self.current_time][transaction_name] = block_info_tuple

                            log.info(key + " already waits for a write lock " + transaction_name + " can not acquire a read lock on "+ variable_name)

                            return
            
            is_acquire = self.site_manager.get_locks(target_transaction, LockType.READ, variable_name)

            if is_acquire == LockAcquireStatus.GotLock or is_acquire == LockAcquireStatus.GotLockRecover:

                if is_acquire == LockAcquireStatus.GotLock:

                    read_value = self.site_manager.get_current_variables(variable_name)
                    log.info(transaction_name + " acquire a read lock on " + variable_name + " with value "+ str(read_value))

                else:

                    log.info("Since " + variable_name +" is the only copy, " + transaction_name + "acquires the read lock on it, although the site hold " + variable_name +" is recovering")

                if variable_name not in target_transaction.variable_read:
                    target_transaction.variable_read[variable_name] = list()

                current_variable_name = self.site_manager.get_current_variables(variable_name)
                target_transaction.variable_read[variable_name].append(current_variable_name)

                self.locktable.add_lock(LockType.Read, target_transaction, variable_name)

                flatten_transaction_wait = self.flatten_transaction_waiting()

                if transaction_name in flatten_transaction_wait:

                    for wait_tuple in flatten_transaction_wait[transaction_name]:
                        if wait_tuple[1] != variable_name:
                            return

                target_transaction.set_status(TransactionStatus.Running)
            
            elif is_acquire == LockAcquireStatus.AllSitesDown:

                wait_tuple = (InstructionType.Read, variable_name)

                flatten_transaction_wait = self.flatten_transaction_waiting()

                if transaction_name in flatten_transaction_wait:

                    if wait_tuple in flatten_transaction_wait[transaction_name]:
                        return
                
                log.info(transaction_name + " is waiting on "+ variable_name)

                target_transaction.set_status(TransactionStatus.Waiting)

                if self.current_time not in self.transaction_list:
                    self.transaction_wait[self.current_time] = {}
                    self.transaction_wait[self.current_time][transaction_name] = wait_tuple
                else:
                    self.transaction_wait[self.current_time][transaction_name] = wait_tuple
            
            else:

                for lock in self.site_manager.get_set_locks().locktable[variable_name]:

                    if lock.transaction == target_transaction:
                        continue

                    transaction_block = lock.transaction.name

                    block_tuple = (transaction_block, InstructionType.Read, variable_name)
                    flatten_transaction_block = self.flatten_transaction_block()

                    if transaction_name in flatten_transaction_block:

                        if block_tuple in flatten_transaction_block[transaction_name]:

                            return

                    log.info(transaction_name + " is blocked by "+ transaction_block + " on variable "+variable_name)

                    target_transaction.set_status(TransactionStatus.Block)

                    if self.current_time not in self.transaction_blocked:
                        self.transaction_blocked[self.current_time] = {}
                        self.transaction_blocked[self.current_time][transaction_name] = block_tuple

                    else:
                        self.transaction_blocked[self.current_time][transaction_name] = block_tuple

                    self.current_time += 1
        return    


                

    def read_only(self, parameters, wait_tag):

        transaction_name = parameters[0]
        variable_name = parameters[1]

        if transaction_name not in self.transaction_list:
            log.info("Transaction " + transaction_name + " not exist")
            return

        target_transaction = self.transaction_list[transaction_name]



        if wait_tag:

            read_value = self.site_manager.get_current_variables(variable_name)

            if read_value is None:

                target_transaction.read_only_values[variable_name] = read_value
            else:
                return

        if variable_name in target_transaction.read_only_values:

            if variable_name not in target_transaction.variable_read:
                target_transaction.variable_read[variable_name] = list()

            target_transaction.variable_read[variable_name].append(target_transaction.read_only_values[variable_name])

            flatten_transaction_wait = self.flatten_transaction_waiting()

            if transaction_name in flatten_transaction_wait:

                for waiting_tuple in flatten_transaction_wait[transaction_name]:

                    if waiting_tuple[1] != variable_name:
                        return

            target_transaction.set_status(TransactionStatus.RUNNING)

        else:

            waiting_txn = (InstructionType.READ_ONLY, variable_name)


            flatten_transaction_wait = self.flatten_transaction_waiting()

            if transaction_name in flatten_transaction_wait:

                if waiting_txn in flatten_transaction_wait[transaction_name]:
                    return

            target_transaction.set_status(TransactionStatus.WAITING)
            log.info(transaction_name + " is waiting on variable " + variable_name)

            if self.current_time not in self.transaction_wait:
                self.transaction_wait[self.current_time] = {}
                self.transaction_wait[self.current_time][transaction_name] = waiting_txn
            else:
                self.transaction_wait[self.current_time][transaction_name] = waiting_txn

        return

    def clear_lock_transaction(self, transaction):

        lock_table = self.site_manager.get_set_locks().get_lock_map()

        for variable_name in sorted(list(lock_table)):
            lock_list = lock_table[variable_name]
            for lock in lock_list:
                if lock.transaction == transaction:
                    self.site_manager.clear_locks(lock, variable_name)
                    log.debug("Clearing locks on site for " + transaction.name +
                              " variable: " + variable_name)

                    if self.locktable.delete_lock(variable_name, lock):
                        log.info("Clearing locks for " + transaction.name +
                                 " variable: " + variable_name)


    def abort(self, transaction_name):
        
        block_pop_list = list()
        wait_pop_list = list()

        for time_step in sorted(self.transaction_blocked.keys()):

            if transaction_name in self.transaction_blocked[time_step]:

                info_tuple = (time_step, transaction_name)
                block_pop_list.append(info_tuple)

        for key in block_pop_list:
            self.transaction_blocked[key[0]].pop(key[1])
        
        for time_step in self.transaction_wait.keys():

            if transaction_name in self.transaction_wait[time_step]:

                info_tuple = (time_step, transaction_name)
                wait_pop_list.append(info_tuple)

        for key in wait_pop_list:

            self.transaction_wait[key[0]].pop(key[1])

        trasaction = self.transaction_list[transaction_name]
        trasaction.set_status(TransactionStatus.Abort)
        self.clear_lock_transaction(trasaction)

        return

    def free_abort(self):

        pop_lis = list()

        for transaction_name in list(self.transaction_list):
            transaction = self.transaction_list[transaction_name]
            if transaction.get_status() == TransactionStatus.Abort:

                pop_lis.append(transaction_name)
                self.abort(transaction_name)

    def deadlock_detect(self, transaction_name, is_visited, current_transaction, blocked_transactions):
        is_abort = self.transaction_list[transaction_name].get_status() == TransactionStatus.Abort
        is_commit = self.transaction_list[transaction_name].get_status() == TransactionStatus.Commit

        if transaction_name in blocked_transactions and not is_commit and not is_abort:
            is_visited[transaction_name] = len(current_transaction) + 1
            current_transaction.append(transaction_name)

            for block_tuple in blocked_transactions[transaction_name]:

                block = block_tuple[0]

                if self.transaction_list[block].get_status() == TransactionStatus.Abort:
                    continue

                if block in is_visited:
                    self.deadlock_clear(current_transaction, is_visited[block] - 1)
                else:
                    self.deadlock_detect(block, is_visited, current_transaction, blocked_transactions)

    def deadlock_clear(self, current_transaction, transaction_index):
        current_transaction = current_transaction[transaction_index:]
        max_index = -1
        name = None

        for transaction_name in current_transaction:
            transaction = self.transaction_list[transaction_name]

            is_abort = self.transaction_list[transaction_name].get_status() == TransactionStatus.Abort
            is_commit = self.transaction_list[transaction_name].get_status() == TransactionStatus.Commit    

            if is_abort or is_commit:
                return

            if max_index < transaction.id:
                max_index = self.transaction_list[transaction_name].id
                name = transaction_name  

        log.info("The youngest transaction "+name+"is aborted")
        self.abort(name)
                    
    def deadlock_detect_and_clear(self):
        flatten_transaction_block = self.flatten_transaction_block()

        for transaction_name in list(flatten_transaction_block):

            is_visited = dict()
            current_transaction = []
            self.deadlock_detect(transaction_name, is_visited, current_transaction, flatten_transaction_block)        

    def block_to_wait(self):

        pop_list = list()
        flatten_transaction_block = self.flatten_transaction_block()

        for block_key in sorted(self.transaction_blocked.keys()):

            items = list(self.transaction_blocked[block_key])

            for key in items:
                info_tuple = self.transaction_blocked[block_key][key]

                is_clear = True

                transaction_block = self.transaction_list[info_tuple[0]]

                is_abort = transaction_block.get_status() == TransactionStatus.ABORTED
                is_commit = transaction_block.get_status() == TransactionStatus.COMMITTED
                is_clear = is_clear & (is_abort or is_commit)

                if is_clear:

                    delete_tuple = None

                    for block_info_tuple in flatten_transaction_block[key]:

                        if block_info_tuple[0] == transaction_block.name:
                            delete_tuple = block_info_tuple
                            break

                    flatten_transaction_block[key].remove(delete_tuple)
                    tuple_append = (block_key, key)
                    pop_list.append(tuple_append)

                    if len(flatten_transaction_block[key]) == 0:

                        flag = False
                        flatten_transaction_wait = self.flatten_transaction_waiting()

                        if key in flatten_transaction_wait:

                            for blk_info_tuple in flatten_transaction_wait[key]:

                                if blk_info_tuple == info_tuple[1:]:

                                    flag = True
                                    break
                        
                        if flag:
                            continue

                        if self.current_time not in self.transaction_wait:
                            self.transaction_wait[self.current_time] = {}
                            self.transaction_wait[self.current_time][key] = info_tuple[1:]
                        transaction = self.transaction_list[key]
                        transaction.set_status(TransactionStatus.Waiting)





    def test_waiting(self):
        pop_list = list()

        for time in list(self.transaction_wait):

            wait_dicts = self.transaction_wait[time]

            for transaction_name in list(wait_dicts):

                parameter = wait_dicts[transaction_name]
                transaction = self.transaction_list[transaction_name]
                transaction.set_status(TransactionStatus.Waiting)

                if parameter[0] == InstructionType.Write:
                    self.write((transaction_name, parameter[1], parameter[2]))

                elif parameter[0] == InstructionType.Read:
                    self.read((transaction_name, parameter[1]))

                elif parameter[0] == InstructionType.ReadOnly:

                    self.read((transaction_name, parameter[1]), wait_tag=True)
                
                if self.transaction_list[transaction_name].get_status() == TransactionStatus.Running:
                    pop_list.append((time, transaction_name))
        
        for key in pop_list:
            self.transaction_wait[key[0]].pop(key[1])


    def commit(self, transaction_name):
        transaction_status = self.transaction_list[transaction_name].get_status()

        if transaction_status == TransactionStatus.Commit or transaction_status == TransactionStatus.Abort:
            return

        transaction = self.transaction_list[transaction_name]
        variable_read = transaction.get_variable_read()

        for variable, value_list in variable_read.items():

            for value in value_list:
                log.info(transaction_name + " read the variable " + variable + " with value "+ value)

        value_commit = transaction.get_variable_commit()

        for variable, value_list in value_commit.items():
            for i in range(1, 21):
                value = int(variable[1:])
                if value % 2 == 0 or ((value % 10) + 1) == i:
                    site = self.site_manager.get_site(i)
                    site.write_variable(transaction,variable,value)
        self.transaction_list[transaction_name].set_status(TransactionStatus.Commit)


    def end(self, parameters):

        transaction_name = parameters[0]
        transaction_status = self.transaction_list[transaction_name].get_status()

        if transaction_status == TransactionStatus.Commit or transaction_status == TransactionStatus.Abort:
            return
        
        self.commit(transaction_name)

        log.info(transaction_name + " committed")
        self.clear_lock_transaction(self.transaction_list[transaction_name])

        flatten_transaction_block = self.flatten_transaction_block()

        pop_block_list = list()
        pop_wait_list = list()

        if transaction_name in flatten_transaction_block:

            flatten_transaction_block.pop(transaction_name)

            for block_dict in sorted(self.transaction_blocked.keys()):

                if transaction_name in self.transaction_blocked[block_dict]:
                    pop_block_list.append((block_dict, transaction_name))
            
            for key in pop_block_list:
                self.transaction_blocked[key[0]].pop(key[1])

        for time in self.transaction_wait.keys():

            if transaction_name in self.transaction_wait[time]:
                pop_wait_list.append((time, transaction_name))

        for key in pop_wait_list:
            self.transaction_wait[key[0]].pop(key[1])

        self.deadlock_detect_and_clear()
        self.block_to_wait()
        self.test_waiting()


