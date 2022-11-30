

class SiteManager:
    '''
    site_numbers: the numbers of sites
    variables_numbers: the total numbers of variables
    '''

    def __init__(self, site_numbers, variables_numbers):
        self.site_numbers = site_numbers
        # Add None to the zero index for quick retrieval.
        self.sites = [None] + [Site(i) for i in range(1, site_numbers + 1)]
        self.variables_numbers = variables_numbers


    def tick(self, instructions):
        """
        instruction: Next instruction object instruction
        """

        parameters = list(instructions.get_parameters())

        if instructions.get_instruction_type() == dumpFunction:
            if len(parameters[0]) == 0:
                for site in self.sites[1:]:
                    site.site_dump()

            elif parameters[0][0] == 'x':
                sites = Variable.get_sites(int(parameters[0][1:]))
                sites = self.get_site_range(sites)

                for site in sites:
                    variables = self.sites[site].get_all_variables()

                    for variable in variables:
                        if variable.name == parameters[0]:
                            log.info(variable.value)

            elif len(parameters[0]) == 2:
                site = self.get_site(int(parameters[0]))
                site.site_dump()

        elif instructions.get_instruction_type() == failFunction:
            self.fail(int(parameters[0]))

        elif instructions.get_instruction_type() == recoverFunction:
            self.recover(int(parameters[0]))

        return


    def get_site_range(self, sites):
        if sites == 'all':
            sites = range(1, self.site_numbers + 1)
        else:
            sites = [sites]
        return sites
    

    def get_site(self, index):
        self.check_index_valid(index)
        return self.sites[index]

    # def start(self):
    #     """
    #     Starts all of the sites
    #     """
    #     for site in self.sites[1:]:
    #         site.listen()

    def fail(self, index):
        """
        Fail a particular site

        Args:
         index: Index of the site to be failed
        """
        self.check_index_valid(index)
        log.info("Site " + str(index) + " failed")
        self.sites[index].fail()

    def recover(self, index):
        """
        Recover a particular site

        Args:
         index: Index of the site to be recovered
        """

        self.check_index_valid(index)
        log.info("Site " + str(index) + " recovered")
        self.sites[index].recover()

    def check_index_valid(self, index):
        if index <= 0 or index > self.site_numbers:
            raise ValueError("Index must be in range %d to %d" %
                             (1, self.site_numbers))








    def get_locks_for_transaction(self, transaction, typelock, variable):
        """
        Tries to provide a lock for a variable to a transaction
        Various check ensure that if a legitimate lock can be
        provided to the transaction

        Args:
            transaction: Transaction which wants the lock
            typelock: Type of lock to be acquired WRITE or READ
            variable: variable name on which lock is requested.
        Returns:
            Boolean telling whether a lock was successfully
            acquired or not
        """
        sites = Variable.get_sites(variable)
        sites = self.get_site_range(sites)

        flag = 1
        recoverFlag = 0
        all_sites_down = 1
        indexEven = int(variable[1:]) % 2 == 0

        for site in sites:

            status = self.sites[site].get_status()
            if status == SiteStatus.Down:
                continue

            if status == SiteStatus.Recover and typelock == LockType.Read:

                if variable not in self.sites[site].variable_recovered:
                    continue

                elif not indexEven:
                    recoverFlag = 1

            all_sites_down = 0

            state = self.sites[site].get_lock_for_transaction(transaction, typelock, variable)

            if state == 1 and typelock == LockType.Read:

                if recoverFlag:
                    return LockAcquireStatus.GotLockRecover
                else:
                    return LockAcquireStatus.GOT_LOCK
            flag &= state

        if all_sites_down == 1:
            return LockAcquireStatus.AllSitesDown
        elif flag == 0:
            return LockAcquireStatus.NoLock
        else:
            return LockAcquireStatus.GotLock



    def get_current_variables(self, var=None):
        """
        Returns currently set variables of for a variable if passed, else
        all of them

        Args:
            var: If none, all variable values will be returned else
                 value will be returned for var
        Returns:
            dict containing values for variables
        """
        variable_values = dict()

        for site in self.sites[1:]:

            if site.status == SiteStatus.Up:
                variables = site.get_all_variables()

                for variable in variables:

                    if var is not None and variable.name == var:
                        return variable.value

                    variable_values[variable.name] = variable.value

                if len(variable_values) == self.variables_numbers:
                    return variable_values

            elif site.status == SiteStatus.Recover:

                variables = site.get_all_variables()

                for variable in variables:

                    if variable.name in site.variable_recovered:

                        if var is not None and variable.name == var:
                            return variable.value

                        variable_values[variable.name] = variable.value

            if len(variable_values) == self.variables_numbers:
                return variable_values

        if var is None:
            return variable_values
        else:
            return None


    def get_set_locks(self):
        """
        Utility function to get all of the locks set in any
        of the site's data manager

        Returns:
            A dict containing variable name as key and list
            of locks present anywhere.
        """
        locks = dict()

        for site in self.sites[1:]:
            lock_map = site.data_manager.lock_table.locktable

            for var, curr_locks in lock_map.items():
                if var not in locks:
                    locks[var] = []

                for lock in curr_locks:
                    if lock not in locks[var]:
                        locks[var].append(lock)
        lock_table = LockTable()
        lock_table.locktable = locks
        return lock_table



    def clear_locks(self, lock, variable_name):
        """
        Clears a particular lock for for a variable

        Args:
            lock: Lock to be cleared
            variable_name: Variable for which the lock is to
                           be cleared
        """

        sites = Variable.get_sites(variable_name)
        sites = self.get_site_range(sites)

        for index in sites:
            site = self.sites[index]
            site.clear_lock(lock, variable_name)


    