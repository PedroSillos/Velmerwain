from load_player import load_player
from load_match_id import load_match_id
from list_data import list_data

def main():
    action = input("\nEnter action: ")
    
    if action.upper() in ["1", "LOAD PLAYER"]:
        load_player()
        return
    if action.upper()  in ["2", "LOAD MATCH ID"]:
        load_match_id()
        return
    if action.upper() in ["3", "LIST"]:
        list_data()
        return
    
    print(f"Invalid action: '{action}'. Bye.")

if __name__ == "__main__":
    main()