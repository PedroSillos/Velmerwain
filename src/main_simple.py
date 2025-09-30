from add_player import add_player
from list_data import list_data

def main():
    action = input("Enter 'add' to add player or 'list' to show all players: ")
    
    if action.upper() == "ADD":
        add_player()
        return
    if action.upper() == "LIST":
        list_data()
        return
    else:
        print(f"Invalid action: "{action}". Bye.")

if __name__ == "__main__":
    main()