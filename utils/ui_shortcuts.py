import tkinter as tk
from tkinter import ttk
import pandas as pd

class GridShortcutManager:
    """Provides Excel-style keyboard shortcuts for ttk.Treeview widgets."""
    
    @staticmethod
    def apply_to(tree: ttk.Treeview):
        """Applies Excel-style shortcuts to the given Treeview."""
        tree.bind("<Control-Down>", lambda e: GridShortcutManager._jump_to_end(tree, "down"))
        tree.bind("<Control-Up>", lambda e: GridShortcutManager._jump_to_end(tree, "up"))
        tree.bind("<Control-Shift-Down>", lambda e: GridShortcutManager._select_to_end(tree, "down"))
        tree.bind("<Control-Shift-Up>", lambda e: GridShortcutManager._select_to_end(tree, "up"))
        tree.bind("<Control-c>", lambda e: GridShortcutManager._copy_to_clipboard(tree))
        tree.bind("<Control-C>", lambda e: GridShortcutManager._copy_to_clipboard(tree))
        tree.bind("<Control-a>", lambda e: GridShortcutManager._select_all(tree))
        tree.bind("<Control-A>", lambda e: GridShortcutManager._select_all(tree))
        
        # Add visual feedback/focus management
        tree.bind("<FocusIn>", lambda e: tree.configure(style="Focused.Treeview"))
        tree.bind("<FocusOut>", lambda e: tree.configure(style="Treeview"))

    @staticmethod
    def _jump_to_end(tree: ttk.Treeview, direction: str):
        """Jumps selection to the first or last row."""
        items = tree.get_children()
        if not items: return
        
        target = items[-1] if direction == "down" else items[0]
        tree.selection_set(target)
        tree.see(target)
        tree.focus(target)

    @staticmethod
    def _select_to_end(tree: ttk.Treeview, direction: str):
        """Selects all rows from current selection to the first or last row."""
        items = tree.get_children()
        if not items: return
        
        current = tree.focus() or (tree.selection()[0] if tree.selection() else items[0])
        curr_idx = items.index(current)
        
        if direction == "down":
            range_items = items[curr_idx:]
        else:
            range_items = items[:curr_idx + 1]
            
        tree.selection_add(range_items)
        tree.see(range_items[-1] if direction == "down" else range_items[0])

    @staticmethod
    def _select_all(tree: ttk.Treeview):
        """Selects all items in the grid."""
        items = tree.get_children()
        if items:
            tree.selection_set(items)

    @staticmethod
    def _copy_to_clipboard(tree: ttk.Treeview):
        """Copies selected rows to the system clipboard in TSV format."""
        selection = tree.selection()
        if not selection: return
        
        # Get column headings
        cols = tree["columns"]
        headings = [tree.heading(c)["text"] for c in cols]
        
        data = []
        for item_id in selection:
            values = tree.item(item_id)["values"]
            # Convert values to string and escape tabs/newlines
            row = [str(v).replace("\t", " ").replace("\n", " ") for v in values]
            data.append("\t".join(row))
            
        header_str = "\t".join(headings)
        content_str = "\n".join(data)
        
        final_str = f"{header_str}\n{content_str}"
        
        # Use Tkinter's clipboard management
        tree.clipboard_clear()
        tree.clipboard_append(final_str)
        tree.update() # Required to finalize clipboard on some platforms
