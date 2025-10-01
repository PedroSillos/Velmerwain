from load_player import load_player
from add_player import add_player
from list_data import list_data

def main():
    action = input("\nEnter 'load' or 'list': ")
    
    if action.upper() == "LOAD":
        load_player()
        return
    #if action.upper() == "ADD":
    #    add_player()
    #    return
    if action.upper() == "LIST":
        list_data()
        return
    else:
        print(f"Invalid action: '{action}'. Bye.")

if __name__ == "__main__":
    main()