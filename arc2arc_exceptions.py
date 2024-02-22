class MakingNewDistributorFirstTimeException(Exception):
    def __init__(self, message="A primer script has just attempted to make a new distributor. If this was the first time, the error here is expected and will be cleared if you run this exact same script one more time."):
        self.message = message
        super().__init__(self.message)

class ArcObjectToMigrationCenterFailed(Exception):
    def __init__(self, message="An error occurred while posting the object to migration center and the object was not created."):
        self.message = message
        super().__init__(self.message)

class ArcRedirectAlreadyExistsFailed(Exception):
    def __init__(self, message="The redirect url already exists and cannot be created or overwritten."):
        self.message = message
        super().__init__(self.message)