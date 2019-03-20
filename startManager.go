package main

import (
    "manager"
    "configure"
)

func main()  {
    configure := "/db_configuration"
    manager := manager.GetManager(configure)
    manager.JoinPresident()
}
