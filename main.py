from web_scraper import DeFiLlamaScraper


def display_menu():
    print("\nDeFiLlama Automated Chains Data Scraper")
    print("=" * 50)
    print("1. Run scraper once")
    print("2. Start scheduled scraper")
    print("3. Export data (CSV)")
    print("4. Export data (JSON)")
    print("5. Export data (Excel)")
    print("6. Show configuration")
    print("7. Exit")
    print("-" * 50)


def main():
    print("DeFiLlama Automated Chains Data Scraper")
    print("=" * 50)

    try:
        scraper = DeFiLlamaScraper()
    except Exception as e:
        print(f"Error initializing scraper: {e}")
        return

    config_summary = scraper.get_config_summary()
    print(f"Configuration loaded:")
    for key, value in config_summary.items():
        print(f"- {key.replace('_', ' ').title()}: {value}")
    print()

    while True:
        display_menu()
        choice = input("Enter your choice (1-7): ").strip()

        if choice == "1":
            print("\nRunning scraper once...")
            result = scraper.run_once()
            if result:
                print("Scraping completed successfully!")
            else:
                print("Scraping failed!")

        elif choice == "2":
            print("\nStarting scheduled scraper...")
            scraper.start_scheduler()

        elif choice == "3":
            print("\nExporting data to CSV...")
            success = scraper.export_data(format_type="csv")
            if success:
                print("Data exported to CSV successfully!")
            else:
                print("CSV export failed!")

        elif choice == "4":
            print("\nExporting data to JSON...")
            success = scraper.export_data(format_type="json")
            if success:
                print("Data exported to JSON successfully!")
            else:
                print("JSON export failed!")

        elif choice == "5":
            print("\nExporting data to Excel...")
            success = scraper.export_data(format_type="xlsx")
            if success:
                print("Data exported to Excel successfully!")
            else:
                print("Excel export failed!")

        elif choice == "6":
            print("\nCurrent Configuration:")
            print("-" * 30)
            config_summary = scraper.get_config_summary()
            for key, value in config_summary.items():
                print(f"{key.replace('_', ' ').title():<20}: {value}")

        elif choice == "7":
            print("Exiting... Goodbye!")
            break

        else:
            print("Invalid choice. Please enter a number between 1-7.")

        if choice != "2":
            input("\nPress Enter to continue...")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nApplication interrupted by user. Exiting...")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        print("Please check the logs for more details.")
