#!/usr/bin/env python3
"""
Initialize the database dashboard with a default developer user.
Run this once to set up the initial admin user.
"""

from hybriddb.storage.query_history_store import register_user

def main():
    print("Initializing database dashboard...")

    # Create default developer user
    success, message = register_user("admin", "admin123", "developer")
    if success:
        print("[OK] Default developer user created:")
        print("   Username: admin")
        print("   Password: admin123")
        print("   Role: developer")
        print("\nPlease change the password after first login!")
    else:
        print(f"[--] Admin user not created: {message}")

    # Create a sample normal user
    success2, message2 = register_user("user", "user123", "user")
    if success2:
        print("\n[OK] Sample normal user created:")
        print("   Username: user")
        print("   Password: user123")
        print("   Role: user")
    else:
        print(f"[--] Sample user not created: {message2}")

if __name__ == "__main__":
    main()