#[cfg(test)]
mod tests {
    use crate::page::Page;
    use common::ids::Lsn;
    use common::testutil::init;

    /// Limits how on how many bytes we can use for page metadata / header

    #[test]
    fn base_page_create() {
        init();
        let p = Page::new(0);
        assert_eq!(0, p.get_page_id());

        let p = Page::new(1);
        assert_eq!(1, p.get_page_id());

        let p = Page::new(1023);
        assert_eq!(1023, p.get_page_id());
    }

    #[test]
    fn base_page_set_lsn() {
        init();
        let mut p = Page::new(1);

        // Check the initial LSN
        assert_eq!(1, p.get_page_id());
        assert_eq!(0, p.get_lsn().page_id);
        assert_eq!(0, p.get_lsn().slot_id);

        // Set the LSN to a new value
        let lsn = Lsn::new(1, 2);
        p.set_lsn(lsn);
        assert_eq!(1, p.get_lsn().page_id);
        assert_eq!(2, p.get_lsn().slot_id);

        // Set the LSN to a new value
        let lsn = Lsn::new(1, 3);
        p.set_lsn(lsn);
        assert_eq!(1, p.get_lsn().page_id);
        assert_eq!(3, p.get_lsn().slot_id);

        // Update again.
        let lsn = Lsn::new(3, 4);
        p.set_lsn(lsn);
        assert_eq!(3, p.get_lsn().page_id);
        assert_eq!(4, p.get_lsn().slot_id);

        // Smaller LSN should not result in a change
        let lsn = Lsn::new(1, 3);
        p.set_lsn(lsn);
        assert_eq!(3, p.get_lsn().page_id);
        assert_eq!(4, p.get_lsn().slot_id);

        let lsn = Lsn::new(2, 5);
        p.set_lsn(lsn);
        assert_eq!(3, p.get_lsn().page_id);
        assert_eq!(4, p.get_lsn().slot_id);
    }

    #[test]
    fn base_page_set_checksum() {
        init();
        let mut p = Page::new(1);
        assert_eq!(0, p.get_checksum());

        // Set the checksum
        p.set_checksum();
        let checksum = p.get_checksum();
        assert_ne!(0, checksum);

        // Should get the same checksum
        p.set_checksum();
        assert_eq!(checksum, p.get_checksum());

        // Change the data, the checksum should change
        p.data[128] = 0xFF;
        p.set_checksum();
        assert_ne!(checksum, p.get_checksum());
    }
}
